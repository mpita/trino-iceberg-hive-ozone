FROM apache/ozone:2.0.0 AS ozone-source

FROM trinodb/trino:478
USER root

# Copiar TODOS los JARs de Ozone al plugin de Iceberg
COPY --from=ozone-source /opt/hadoop/share/ozone/lib/*.jar /usr/lib/trino/plugin/iceberg/

# Copiar configuración de Hadoop y catálogo Iceberg
COPY core-site.xml /etc/trino/core-site.xml
COPY catalog/iceberg.properties /etc/trino/catalog/iceberg.properties

USER trino
