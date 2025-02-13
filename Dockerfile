FROM ghcr.io/apache/flink-docker:1.18-SNAPSHOT-scala_2.12-java11-debian

RUN rm -rf /opt/flink
USER flink
ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache
COPY --chown=flink:flink flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/ /opt/flink

RUN mkdir /opt/flink/plugins/flink-s3-fs-hadoop; \
  cp /opt/flink/opt/flink-s3-fs-hadoop-1.18-SNAPSHOT.jar /opt/flink/plugins/flink-s3-fs-hadoop/
ENV FLINK_HOME=/opt/flink

# Replace default REST/RPC endpoint bind address to use the container's network interface
RUN sed -i 's/rest.address: localhost/rest.address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i 's/rest.bind-address: localhost/rest.bind-address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i 's/jobmanager.bind-host: localhost/jobmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i 's/taskmanager.bind-host: localhost/taskmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i '/taskmanager.host: localhost/d' $FLINK_HOME/conf/flink-conf.yaml;
