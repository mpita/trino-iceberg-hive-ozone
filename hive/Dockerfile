FROM curlimages/curl:8.11.0 AS builder
RUN curl -L https://repo1.maven.org/maven2/org/apache/ozone/ozone-filesystem-hadoop3/2.0.0/ozone-filesystem-hadoop3-2.0.0.jar -o /tmp/ozone-filesystem-hadoop3.jar
RUN curl -L https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar -o /tmp/postgresql-jdbc.jar

FROM apache/hive:4.0.0
USER root
COPY --from=builder /tmp/ozone-filesystem-hadoop3.jar /opt/hive/lib/ozone-filesystem-hadoop3.jar
COPY --from=builder /tmp/postgresql-jdbc.jar /opt/hive/lib/postgresql-jdbc.jar
USER hive
