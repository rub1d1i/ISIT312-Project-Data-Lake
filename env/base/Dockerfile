# Single-node Hadoop that mirrors the ISIT312 BigDataVM (Hadoop 2.7.3 in /usr/share/hadoop),
# so the lab commands run unchanged on Apple Silicon.
FROM eclipse-temurin:8-jdk-jammy

ARG HADOOP_VERSION=2.7.3
ARG HADOOP_SHA256=d489df3808244b906eb38f4d081ba49e50c4603db03efd5e594a1e98b09259c2

ENV HADOOP_HOME=/usr/share/hadoop \
    HADOOP_CONF_DIR=/usr/share/hadoop/etc/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl procps nano less unzip \
 && rm -rf /var/lib/apt/lists/*

# Same account as the VM (user and password are both "bigdata").
# /media/sf_vmshare is where VirtualBox mounts the host's shared folder; Docker mounts ours there too.
RUN useradd -m -s /bin/bash bigdata && echo 'bigdata:bigdata' | chpasswd \
 && mkdir -p /media/sf_vmshare && chown bigdata:bigdata /media/sf_vmshare

RUN curl -fSL --retry 3 -o /tmp/hadoop.tgz https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
 && echo "${HADOOP_SHA256}  /tmp/hadoop.tgz" | sha256sum -c - \
 && tar -xzf /tmp/hadoop.tgz -C /usr/share \
 && mv /usr/share/hadoop-${HADOOP_VERSION} $HADOOP_HOME \
 && rm -rf /tmp/hadoop.tgz $HADOOP_HOME/share/doc \
 && echo "export JAVA_HOME=${JAVA_HOME}" >> $HADOOP_CONF_DIR/hadoop-env.sh \
 && chown -R bigdata:bigdata $HADOOP_HOME

COPY conf/*.xml $HADOOP_CONF_DIR/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
# /usr/local/bin comes before /usr/bin on PATH, so Hadoop picks up this kill instead of procps-ng's.
COPY kill-wrapper.sh /usr/local/bin/kill

# Week 4 lab: Python 3 for Hadoop Streaming, and sudo so the lab's "sudo apt-get ..." lines run
# (the VM asks for the bigdata password; here it is passwordless). Kept after the Hadoop download
# so a rebuild reuses that cached layer. The package lists stay so apt-get works inside the container.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 sudo \
 && echo 'bigdata ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/bigdata \
 && chmod 440 /etc/sudoers.d/bigdata

# Week 5 lab: Hive in /usr/share/hive, as on the VM, with its Derby metastore in /usr/share/hive/metastore_db.
ARG HIVE_VERSION=2.3.9
ARG HIVE_SHA256=1996327c96672e7ea8eaa8fc63972c75bc095287a32612e545e0e51c1622cb5c
RUN curl -fSL --retry 3 -o /tmp/hive.tgz https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
 && echo "${HIVE_SHA256}  /tmp/hive.tgz" | sha256sum -c - \
 && tar -xzf /tmp/hive.tgz -C /usr/share \
 && mv /usr/share/apache-hive-${HIVE_VERSION}-bin /usr/share/hive \
 && rm -f /tmp/hive.tgz \
 && chown -R bigdata:bigdata /usr/share/hive
ENV HIVE_HOME=/usr/share/hive \
    HIVE_CONF_DIR=/usr/share/hive/conf
ENV PATH=$HIVE_HOME/bin:$PATH
COPY --chown=bigdata:bigdata conf/hive/hive-site.xml $HIVE_CONF_DIR/

USER bigdata
# Hadoop's daemon scripts name their log/pid files after $USER, which Docker leaves unset.
ENV USER=bigdata
WORKDIR /home/bigdata

# Format HDFS and create the bigdata home directory (/user/bigdata) up front,
# so relative paths like "myfolder" work exactly as they do in the VM.
RUN mkdir -p Desktop Documents hadoop-data \
 && hdfs namenode -format -force -nonInteractive \
 && hadoop-daemon.sh start namenode \
 && hadoop-daemon.sh start datanode \
 && hdfs dfsadmin -safemode wait \
 && hdfs dfs -mkdir -p /user/bigdata /tmp \
 && hdfs dfs -chmod 1777 /tmp \
 && hadoop-daemon.sh stop datanode \
 && hadoop-daemon.sh stop namenode \
 && rm -rf /tmp/*.pid /tmp/hsperfdata_bigdata $HADOOP_HOME/logs/*

# Create the empty Hive metastore (the VM ships with it already initialised).
RUN cd $HIVE_HOME && schematool -dbType derby -initSchema && rm -f derby.log

# 50070 NameNode UI, 50075 DataNode UI, 8088 YARN UI, 8042 NodeManager UI, 19888 Job History UI
EXPOSE 50070 50075 8088 8042 19888

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["bash", "-l"]
