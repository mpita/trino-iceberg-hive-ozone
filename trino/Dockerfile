FROM apache/ozone:2.0.0 AS ozone-source

FROM trinodb/trino:478
USER root

# Copiar el JAR del sistema de archivos de Ozone (versión completa, no client, para asegurar compatibilidad)
COPY --from=ozone-source /opt/hadoop/share/ozone/lib/ozone-filesystem-hadoop3-2.0.0.jar /usr/lib/trino/plugin/iceberg/hdfs/

# Copiar configuración de Hadoop y catálogo Iceberg
COPY core-site.xml /etc/trino/core-site.xml
COPY catalog/iceberg.properties /etc/trino/catalog/iceberg.properties

USER trino
