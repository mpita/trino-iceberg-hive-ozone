FROM curlimages/curl:8.11.0 AS builder
RUN curl -L https://repo1.maven.org/maven2/org/apache/ozone/ozone-filesystem-hadoop3/2.0.0/ozone-filesystem-hadoop3-2.0.0.jar -o /tmp/ozone-filesystem-hadoop3.jar

FROM trinodb/trino:478
USER root
COPY --from=builder /tmp/ozone-filesystem-hadoop3.jar /usr/lib/trino/plugin/iceberg/ozone-filesystem-hadoop3.jar
USER trino
