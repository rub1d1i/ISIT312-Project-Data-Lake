# ISIT312 data lake image: the course lab image (Hadoop 2.7.3, Hive 2.3.9, HBase 1.2.6, JDK 8)
# plus Spark 2.4.8, Kafka 2.8.2 and the Spark<->Kafka connector. Everything from the base image
# is kept as it is (user bigdata, same paths); only yarn-site.xml, mapred-site.xml and
# capacity-scheduler.xml are replaced by the copies in conf/hadoop/.
#
#   docker --context colima build -t isit312-datalake:latest .        (or: ./lake.sh build)
FROM isit312-hadoop:latest

USER root

# netcat for port checks / quick socket tests. curl and less are already in the base image;
# they are listed so the image still has them if the base ever changes. The package lists are
# kept (as in the base image) so `sudo apt-get install ...` works inside the container.
RUN apt-get update \
 && apt-get install -y --no-install-recommends netcat-openbsd curl less

# ---- Apache Spark 2.4.8, prebuilt for Hadoop 2.7 (Scala 2.11) --------------------------------
# SHA-512 as published in https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz.sha512
ARG SPARK_VERSION=2.4.8
ARG SPARK_SHA512=752c4d4d8fe1d72f5ba01f40d22df35698585bd17ed4749f6065b0039ff40db7ff8ea87dc0fb5b1ec03871e427a002581ec12f486392b92b88643d4243908e55
RUN curl -fSL --retry 3 -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
 && echo "${SPARK_SHA512}  /tmp/spark.tgz" | sha512sum -c - \
 && tar -xzf /tmp/spark.tgz -C /usr/share \
 && mv /usr/share/spark-${SPARK_VERSION}-bin-hadoop2.7 /usr/share/spark \
 && rm -f /tmp/spark.tgz \
 && chown -R bigdata:bigdata /usr/share/spark

# ---- Apache Kafka 2.8.2 (Scala 2.12 build; the broker's Scala version is independent of Spark's)
# SHA-512 as published in https://archive.apache.org/dist/kafka/2.8.2/kafka_2.12-2.8.2.tgz.sha512
ARG KAFKA_VERSION=2.8.2
ARG KAFKA_SHA512=ce90f4722d49d896643ce63e1b4f8f1620c7a3dca603646733052b3a9155f3e3f7e93422015c0c588988be836487f9d638472a624f1c4895662daa3aa3ba2bb7
RUN curl -fSL --retry 3 -o /tmp/kafka.tgz https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.12-${KAFKA_VERSION}.tgz \
 && echo "${KAFKA_SHA512}  /tmp/kafka.tgz" | sha512sum -c - \
 && tar -xzf /tmp/kafka.tgz -C /usr/share \
 && mv /usr/share/kafka_2.12-${KAFKA_VERSION} /usr/share/kafka \
 && rm -rf /tmp/kafka.tgz /usr/share/kafka/site-docs \
 && chown -R bigdata:bigdata /usr/share/kafka

# ---- Kafka source/sink for Structured Streaming, available offline ---------------------------
# spark-sql-kafka-0-10 for Spark 2.4.8 / Scala 2.11 and the kafka-clients version it is built
# against (2.0.0). Its other dependencies (lz4, snappy, slf4j) already ship with Spark.
# SHA-1 values as published next to the jars on Maven Central (*.jar.sha1).
ARG SPARK_KAFKA_SHA1=aa9be74fb40ff0d08c07c8c143abd9201c52d454
ARG KAFKA_CLIENTS_VERSION=2.0.0
ARG KAFKA_CLIENTS_SHA1=48bcf5ebb6effdaffbc2f725aae954f764d25341
RUN cd /usr/share/spark/jars \
 && curl -fSL --retry 3 -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/${SPARK_VERSION}/spark-sql-kafka-0-10_2.11-${SPARK_VERSION}.jar \
 && curl -fSL --retry 3 -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
 && echo "${SPARK_KAFKA_SHA1}  spark-sql-kafka-0-10_2.11-${SPARK_VERSION}.jar" | sha1sum -c - \
 && echo "${KAFKA_CLIENTS_SHA1}  kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" | sha1sum -c - \
 && chown bigdata:bigdata spark-sql-kafka-0-10_2.11-${SPARK_VERSION}.jar kafka-clients-${KAFKA_CLIENTS_VERSION}.jar

# ---- Snappy on Apple Silicon (linux/aarch64) -------------------------------------------------
# The snappy-java jars bundled with Hadoop 2.7.3 (1.0.4.1), HBase 1.2.6 (1.0.4.1) and Hive 2.3.9
# (1.1.1.3) contain no native library for linux/aarch64, so on this image every Hadoop/Hive JVM
# that touches Snappy dies with "SnappyError: [FAILED_TO_LOAD_NATIVE_LIBRARY]". That breaks the
# central data-lake path: Spark writes Parquet with Snappy by default, and Hive (MapReduce
# tasks and HiveServer2) could not read those tables. (On the x86 lab VM the old jars work.)
# Fix: put Spark's snappy-java 1.1.8.2, which does ship an aarch64 library and keeps the same
# API, in place of the old jar on those classpaths. `test -f` makes the build fail loudly if
# the base image ever changes underneath this.
RUN set -e; new=/usr/share/spark/jars/snappy-java-1.1.8.2.jar; test -f "$new"; \
    for old in /usr/share/hadoop/share/hadoop/common/lib/snappy-java-1.0.4.1.jar \
               /usr/share/hadoop/share/hadoop/mapreduce/lib/snappy-java-1.0.4.1.jar \
               /usr/share/hadoop/share/hadoop/tools/lib/snappy-java-1.0.4.1.jar \
               /usr/share/hbase/lib/snappy-java-1.0.4.1.jar \
               /usr/share/hive/lib/snappy-java-1.1.1.3.jar; do \
      test -f "$old"; rm "$old"; cp -p "$new" "$(dirname "$old")/"; \
    done

ENV SPARK_HOME=/usr/share/spark \
    KAFKA_HOME=/usr/share/kafka
# Appended (not prepended) on purpose: Spark ships its own old `beeline` in bin/, and Hive's
# beeline must stay the one found first. Spark's sbin/ is left off the PATH because its
# start-all.sh/stop-all.sh would shadow Hadoop's.
ENV PATH=$PATH:$SPARK_HOME/bin:$KAFKA_HOME/bin

# ---- Configuration ---------------------------------------------------------------------------
# YARN/MapReduce sizing for this single node (replaces the three base files; see the comments
# inside each file for what differs from the base image).
COPY --chown=bigdata:bigdata conf/hadoop/yarn-site.xml conf/hadoop/mapred-site.xml conf/hadoop/capacity-scheduler.xml $HADOOP_CONF_DIR/
COPY --chown=bigdata:bigdata conf/spark/ $SPARK_HOME/conf/
COPY --chown=bigdata:bigdata conf/kafka/server.properties conf/kafka/zookeeper.properties $KAFKA_HOME/config/
# A directory holding nothing but hbase-site.xml, for Spark's classpath. (Putting
# /usr/share/hbase/conf itself on the classpath would also pull in HBase's log4j.properties
# and switch spark-shell to noisy INFO logging.)
RUN mkdir -p /usr/share/spark/hbase-conf \
 && ln -s /usr/share/hbase/conf/hbase-site.xml /usr/share/spark/hbase-conf/hbase-site.xml \
 && chown -R bigdata:bigdata /usr/share/spark/hbase-conf \
 && install -d -o bigdata -g bigdata /home/bigdata/logs /home/bigdata/kafka-data /home/bigdata/project

# Service control script used by lake.sh (and usable directly inside the container).
COPY --chmod=755 lakectl /usr/local/bin/lakectl

# 10000/10002 HiveServer2 + its UI, 16010 HBase Master UI, 4040/4041 Spark UI, 9092 Kafka
EXPOSE 10000 10002 16010 4040 4041 9092

USER bigdata
WORKDIR /home/bigdata
