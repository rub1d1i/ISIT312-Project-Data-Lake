# Lab base image

These files build `isit312-hadoop:latest`, the Docker replica of the subject's lab virtual machine
(user `bigdata`; Hadoop 2.7.3 pseudo-distributed, Hive 2.3.9 with a Derby metastore, HBase 1.2.6, JDK 8).
`../lake.sh build` builds it automatically when it is missing, then builds the lake image on top of it.

By hand:

```bash
docker build -t isit312-hadoop:hadoop-hive -f Dockerfile .
docker build -t isit312-hadoop:latest --build-arg BASE=isit312-hadoop:hadoop-hive -f Dockerfile.hbase .
```

`Dockerfile.hive` is kept for reference only: it adds Hive to an older image that had Hadoop alone.
