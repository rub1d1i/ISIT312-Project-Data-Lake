#!/bin/bash
# lake.sh - run the ISIT312 data lake (Hadoop, Hive, HBase, Spark, Kafka in ONE container)
# inside the dedicated Colima VM. Docker Desktop and its containers are never touched:
# every docker call here goes through  docker --context colima  and the machine's current
# docker context is left as it is.
#
#   ./lake.sh build        build the image isit312-datalake:latest (needs the base image)
#   ./lake.sh up           create/start the container (checks the Mac's ports first)
#   ./lake.sh start-all    start every service and wait until each one answers
#   ./lake.sh start-core | start-hive | start-hbase | start-kafka     start one group only
#   ./lake.sh status       jps + port checks
#   ./lake.sh shell        interactive bash as bigdata
#   ./lake.sh exec '<cmd>' run one command (bash -lc) as bigdata
#   ./lake.sh stop-all     stop every service
#   ./lake.sh down         stop the services and the container (the container is kept)
#
# Written for macOS's stock bash 3.2.
set -u -o pipefail

CTX=colima
NAME=isit312-datalake
IMAGE=isit312-datalake:latest
BASE_IMAGE=isit312-hadoop:latest
BASE_CONTEXT=${LAKE_BASE_CONTEXT:-desktop-linux}   # where `build` can copy the base image from
CONTAINER_HOSTNAME=datalake
# 50070 NameNode  50075 DataNode  8088 YARN  8042 NodeManager  19888 JobHistory
# 10000 HiveServer2 (JDBC)  10002 HiveServer2 UI  16010 HBase Master UI
# 4040/4041 Spark UI (first/second running application)  9092 Kafka
PORTS="50070 50075 8088 8042 19888 10000 10002 16010 4040 4041 9092"
# Published on the Mac's loopback only, so the unauthenticated Hadoop UIs are not offered to
# the whole network. Set LAKE_BIND=0.0.0.0 before the first `up` to publish on all interfaces.
BIND=${LAKE_BIND:-127.0.0.1}
COLIMA_ARGS="--cpu 8 --memory 14 --disk 60 --vm-type vz --mount-type virtiofs --activate=false"

ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$ENV_DIR/.." && pwd)"        # .../project  ->  /home/bigdata/project

command -v colima >/dev/null 2>&1 || PATH="/opt/homebrew/bin:$PATH"

d()   { docker --context "$CTX" "$@"; }
die() { echo "lake.sh: $*" >&2; exit 1; }

# Start the Colima VM if it is not running. --activate=false keeps the current docker context;
# it is checked afterwards anyway and put back if Colima changed it.
ensure_colima() {
  d info >/dev/null 2>&1 && return 0
  command -v colima >/dev/null 2>&1 || die "colima is not installed (brew install colima)"
  local before after
  before=$(docker context show 2>/dev/null)
  echo "The Colima VM is not running - starting it (the docker context stays '$before') ..."
  # shellcheck disable=SC2086
  colima start $COLIMA_ARGS || die "colima start failed"
  after=$(docker context show 2>/dev/null)
  if [ -n "$before" ] && [ "$before" != "$after" ]; then
    echo "Colima changed the docker context to '$after' - putting it back to '$before'"
    docker context use "$before" >/dev/null || die "could not restore docker context '$before'"
  fi
  d info >/dev/null 2>&1 || die "the colima docker context does not answer"
}

state() { d inspect -f '{{.State.Status}}' "$NAME" 2>/dev/null || true; }

need_running() {
  d info >/dev/null 2>&1 || die "the Colima VM is not running - run: ./lake.sh up"
  [ "$(state)" = running ] || die "container $NAME is not running - run: ./lake.sh up"
}

# Report every wanted port that is already taken on the Mac. Returns 1 if there is a clash.
check_ports() {
  local clash=0 p who
  for p in $PORTS; do
    who=$(lsof -nP -iTCP:"$p" -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $1 " (pid " $2 ")"}' | sort -u | paste -sd, -)
    if [ -n "$who" ]; then
      echo "  port $p: IN USE by $who"; clash=1
    elif nc -z -G 1 127.0.0.1 "$p" >/dev/null 2>&1; then
      echo "  port $p: IN USE (something answers on 127.0.0.1:$p; lsof cannot see the owner)"; clash=1
    else
      echo "  port $p: free"
    fi
  done
  return $clash
}

# After the container is up, confirm the Mac really forwards each published port.
verify_forwards() {
  local p missing="" i
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    missing=""
    for p in $PORTS; do
      nc -z -G 1 127.0.0.1 "$p" >/dev/null 2>&1 || missing="$missing $p"
    done
    [ -z "$missing" ] && { echo "all published ports are reachable on localhost"; return 0; }
    sleep 1
  done
  echo "WARNING: these published ports are not reachable on localhost yet:$missing"
  echo "         (Colima forwards a port a few seconds after the container starts;"
  echo "          check again with: nc -z 127.0.0.1 <port>)"
  return 0
}

cmd_build() {
  ensure_colima
  if ! d image inspect "$BASE_IMAGE" >/dev/null 2>&1; then
    if docker --context "$BASE_CONTEXT" image inspect "$BASE_IMAGE" >/dev/null 2>&1; then
      echo "$BASE_IMAGE is not in the colima context - copying it from the '$BASE_CONTEXT' context ..."
      docker --context "$BASE_CONTEXT" save "$BASE_IMAGE" | d load || die "copying the base image failed"
    else
      # the lab image: Hadoop 2.7.3 and Hive 2.3.9 (base/Dockerfile), then HBase 1.2.6 (base/Dockerfile.hbase)
      echo "$BASE_IMAGE not found - building the lab base image from $ENV_DIR/base ..."
      d build -t isit312-hadoop:hadoop-hive -f "$ENV_DIR/base/Dockerfile" "$ENV_DIR/base" || die "base image build (Hadoop, Hive) failed"
      d build -t "$BASE_IMAGE" --build-arg BASE=isit312-hadoop:hadoop-hive -f "$ENV_DIR/base/Dockerfile.hbase" "$ENV_DIR/base" || die "base image build (HBase) failed"
    fi
  fi
  d build -t "$IMAGE" "$ENV_DIR" || die "image build failed"
}

cmd_up() {
  ensure_colima
  local st p publish=""
  st=$(state)
  if [ "$st" = running ]; then
    echo "container $NAME is already running"
    return 0
  fi
  if [ -z "$st" ]; then
    d image inspect "$IMAGE" >/dev/null 2>&1 || die "image $IMAGE not found - run: ./lake.sh build"
  fi
  echo "checking that the Mac's ports are free:"
  check_ports || die "port clash - free the port(s) marked IN USE above, then run ./lake.sh up again. Nothing was started."
  if [ -z "$st" ]; then
    for p in $PORTS; do publish="$publish -p $BIND:$p:$p"; done
    echo "creating container $NAME"
    # --init reaps stopped daemons (see hadoop-docker/hadoop-lab.sh); `sleep infinity` keeps the
    # container alive, the services are started separately with ./lake.sh start-all.
    # shellcheck disable=SC2086
    d run -d --init --name "$NAME" --hostname "$CONTAINER_HOSTNAME" $publish \
      -v "$PROJECT_DIR:/home/bigdata/project" \
      "$IMAGE" sleep infinity >/dev/null || die "docker run failed"
  else
    echo "starting existing container $NAME (was: $st)"
    d start "$NAME" >/dev/null || die "docker start failed"
  fi
  verify_forwards
  echo "container $NAME is up. Next: ./lake.sh start-all"
}

cmd_shell() { need_running; exec docker --context "$CTX" exec -it -u bigdata -w /home/bigdata "$NAME" bash -l; }

cmd_exec() {
  need_running
  [ $# -gt 0 ] || die "usage: ./lake.sh exec '<command>'"
  # -i so input can be piped in, e.g.  ./lake.sh exec 'spark-shell' < script.scala
  d exec -i -u bigdata -w /home/bigdata "$NAME" bash -lc "$*"
}

ctl() { need_running; d exec -u bigdata -w /home/bigdata "$NAME" bash -lc "lakectl $1"; }

cmd_status() {
  d info >/dev/null 2>&1 || { echo "Colima VM: not running (./lake.sh up starts it)"; exit 1; }
  local st; st=$(state)
  echo "container $NAME: ${st:-does not exist}"
  [ "$st" = running ] || exit 1
  d stats --no-stream --format '  memory {{.MemUsage}} ({{.MemPerc}}), cpu {{.CPUPerc}}' "$NAME"
  echo
  d exec -u bigdata -w /home/bigdata "$NAME" bash -lc "lakectl status"
}

cmd_down() {
  d info >/dev/null 2>&1 || { echo "Colima VM is not running - nothing to stop"; return 0; }
  local st; st=$(state)
  if [ "$st" = running ]; then
    d exec -u bigdata -w /home/bigdata "$NAME" bash -lc "lakectl stop-all" || true
    d stop -t 30 "$NAME" >/dev/null || die "docker stop failed"
    echo "container $NAME stopped (kept; ./lake.sh up starts it again). The Colima VM is still running: colima stop"
  else
    echo "container $NAME is not running (state: ${st:-does not exist})"
  fi
}

case "${1:-}" in
  build)  cmd_build ;;
  up)     cmd_up ;;
  shell)  cmd_shell ;;
  exec)   shift; cmd_exec "$@" ;;
  start-core|start-hive|start-hbase|start-kafka|start-all|stop-all) ctl "$1" ;;
  status) cmd_status ;;
  down)   cmd_down ;;
  *)
    sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
    exit 2 ;;
esac
