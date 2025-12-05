FROM maven:3.9-eclipse-temurin-11 AS ozonefs-builder

# Construye el JAR de OzoneFS con el prefijo sombreado de Protobuf que usa Trino
RUN git clone --branch ozone-2.0.0 --depth 1 https://github.com/apache/ozone.git /tmp/ozone \
 && cd /tmp/ozone/hadoop-ozone/ozonefs-hadoop3-client \
 && mvn -DskipTests -Dproto.shaded.prefix='io.trino.hadoop.\$internal' clean package

FROM trinodb/trino:478
USER root

# Copiar el JAR generado con Protobuf sombreado compatible con Trino
COPY --from=ozonefs-builder /tmp/ozone/hadoop-ozone/ozonefs-hadoop3-client/target/ozone-filesystem-hadoop3-client-2.0.0.jar /usr/lib/trino/plugin/iceberg/hdfs/ozone-filesystem-hadoop3-client-2.0.0.jar

# Copiar configuración de Hadoop y catálogo Iceberg
COPY core-site.xml /etc/trino/core-site.xml
COPY catalog/iceberg.properties /etc/trino/catalog/iceberg.properties

USER trino
