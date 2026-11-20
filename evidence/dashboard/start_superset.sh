#!/bin/bash
# Starts Apache Superset in a second container next to the lake and gives it the Hive driver.
# Superset reaches HiveServer2 of the lake container over the Docker bridge (port 10000).
#   bash start_superset.sh     then    python3 build_dashboard.py    and    python3 shot_dashboard.py
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
d() { docker --context colima "$@"; }
d rm -f lake-superset > /dev/null 2>&1 || true
d run -d --name lake-superset -p 127.0.0.1:8090:8088 -e GUNICORN_TIMEOUT=600 -e SERVER_WORKER_AMOUNT=4 \
  -e SUPERSET_CONFIG_PATH=/app/superset_config.py apache/superset:4.1.1 > /dev/null
d cp "$HERE/superset_config.py" lake-superset:/app/superset_config.py
d exec lake-superset sh -c "pip install -q 'pyhive[hive_pure_sasl]' 2>&1 | grep -v notice || true
  superset db upgrade > /dev/null 2>&1
  superset fab create-admin --username admin --firstname Lake --lastname Admin --email admin@example.com --password prototype-admin > /dev/null 2>&1
  superset init > /dev/null 2>&1"
# Superset 4.1 asks every Hive connection for SHOW CATALOGS, which Hive 2.3 does not know:
# tell its Hive driver that there are no catalogs.
d exec -u root lake-superset sh -c "grep -q _no_catalogs /app/superset/db_engine_specs/hive.py || printf '\n\ndef _no_catalogs(cls, database, inspector):\n    return set()\n\n\nHiveEngineSpec.get_catalog_names = classmethod(_no_catalogs)\n' >> /app/superset/db_engine_specs/hive.py"
d restart lake-superset > /dev/null
for i in $(seq 1 60); do curl -sf http://127.0.0.1:8090/health > /dev/null && break; sleep 2; done
echo "Superset answers on http://127.0.0.1:8090 (admin / prototype-admin)"
