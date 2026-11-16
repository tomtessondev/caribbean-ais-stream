# Flink 1.20 (LTS) with the connectors the SQL jobs need: Kafka in (Avro, schemas from the
# Confluent registry), JDBC/PostgreSQL out.
FROM flink:1.20.2-scala_2.12-java17

ARG MAVEN=https://repo1.maven.org/maven2
# zstd-jni: the producer compresses with zstd, and the Kafka connector does not bundle the
# codec (the job fails with ClassNotFoundException: com.github.luben.zstd.BufferPool).
RUN cd /opt/flink/lib \
 && wget -q "$MAVEN/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" \
 && wget -q "$MAVEN/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar" \
 && wget -q "$MAVEN/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar" \
 && wget -q "$MAVEN/com/github/luben/zstd-jni/1.5.6-4/zstd-jni-1.5.6-4.jar" \
 && wget -q "$MAVEN/org/apache/flink/flink-sql-avro-confluent-registry/1.20.2/flink-sql-avro-confluent-registry-1.20.2.jar"

COPY sql /opt/flink/sql
