#!/bin/bash
# Installs Apache Pig 0.17.0 inside the running lake container (user bigdata).
# Pig is NOT part of the proposed architecture; it is installed only so that
# the tool comparison (pipelines/experiments/e1_engines.sh) can measure it
# next to MapReduce, Hive and Spark instead of dismissing it by argument.
#
#   env/lake.sh exec 'bash project/env/install_pig.sh'
#
# The files live in the container's home directory. If the container is
# removed and recreated, run this script again.
set -euo pipefail
PIG_VERSION=0.17.0
DEST=/home/bigdata/pig-$PIG_VERSION
if [ -x "$DEST/bin/pig" ]; then echo "Pig already installed in $DEST"; exit 0; fi
cd /home/bigdata
curl -fSL --retry 3 -o /tmp/pig.tgz https://archive.apache.org/dist/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz
# The archive has no .sha512 file for this release (the URL returns 404), so the .md5 file is tried instead.
if curl -fsSL --retry 3 -o /tmp/pig.tgz.md5 https://archive.apache.org/dist/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz.md5; then
  WANT=$(grep -oiE '[0-9a-f]{32}' /tmp/pig.tgz.md5 | head -1 | tr 'A-F' 'a-f')
  GOT=$(md5sum /tmp/pig.tgz | cut -d' ' -f1)
  [ "$WANT" = "$GOT" ] && echo "MD5 checksum OK" || { echo "MD5 checksum MISMATCH"; exit 1; }
else
  echo "no checksum file published for this release: archive NOT verified"
fi
tar -xzf /tmp/pig.tgz -C /home/bigdata
rm -f /tmp/pig.tgz /tmp/pig.tgz.md5
"$DEST/bin/pig" -version
