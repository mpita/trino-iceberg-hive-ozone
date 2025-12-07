# Plataforma de Datos Local con Trino, Iceberg y Apache Ozone

Este repositorio contiene un entorno de desarrollo local basado en Docker Compose que integra **Trino**, **Apache Iceberg**, **Hive Metastore** y **Apache Ozone**.

El objetivo es proporcionar una plataforma completa para simular un Data Lakehouse moderno en tu máquina local.

## Componentes de la Arquitectura

### 1. Apache Ozone (v2.0.0)
Es un almacén de objetos distribuido y escalable, compatible con el protocolo S3 y HDFS (Hadoop Distributed File System).
*   **Rol**: Actúa como la capa de almacenamiento persistente (Storage Layer). Aquí es donde residen físicamente los archivos de datos (Parquet, Avro, ORC, etc.).
*   **Servicios**:
    *   `om` (Ozone Manager): Gestiona los metadatos de volúmenes, buckets y claves.
    *   `scm` (Storage Container Manager): Gestiona los nodos de datos y la replicación.
    *   `datanode`: Almacena los datos reales.
    *   `s3g`: Gateway para compatibilidad con el protocolo S3.

### 2. Hive Metastore (v4.0.0)
Es el servicio central que almacena los metadatos de las tablas (esquemas, particiones, ubicación de archivos).
*   **Rol**: Catálogo de metadatos. Permite que los motores de consulta (como Trino) sepan qué tablas existen y dónde están sus datos en Ozone.
*   **Configuración**: Está configurado para usar el sistema de archivos `ofs://` de Ozone.

### 3. PostgreSQL (v13)
Base de datos relacional.
*   **Rol**: Backend de persistencia para el **Hive Metastore**. Aquí es donde Hive guarda realmente la información de las tablas para que no se pierda al reiniciar los contenedores.

### 4. Trino (v478)
Motor de consultas SQL distribuido de alto rendimiento.
*   **Rol**: Capa de computación (Compute Layer). Permite ejecutar consultas SQL analíticas sobre los datos almacenados en Ozone.
*   **Conector**: Utiliza el conector de **Iceberg** para leer y escribir datos, comunicándose con el Hive Metastore para obtener la ubicación de las tablas.

### 5. Apache Iceberg
Es un formato de tabla abierto para grandes conjuntos de datos analíticos.
*   **Rol**: Formato de tabla. Proporciona características como transacciones ACID, evolución de esquemas y "time travel" sobre los archivos almacenados en Ozone.

### Servicios en `docker-compose.yml`

| Servicio | Rol breve |
| --- | --- |
| `scm` | Storage Container Manager: orquesta datanodes y la metadata de contenedores. |
| `om` | Ozone Manager: guarda metadata de volúmenes, buckets y claves; expone `ofs://`. |
| `datanode` | Nodo de datos que almacena los bloques/objetos físicos. |
| `s3g` | Gateway S3 para acceder a Ozone vía API S3. |
| `init-ozone` | Job efímero que crea `/vol1` y `/vol1/bucket1` al arranque. |
| `postgres` | Base para Hive Metastore (metadata de tablas). |
| `hive-metastore` | Servicio Hive Metastore 4.0 que publica `thrift://hive-metastore:9083`. |
| `trino` | Coordinador Trino 478 con conector Iceberg y cliente OFS sombreado. |

---

## Requisitos Previos

*   [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y ejecutándose.
*   Git (opcional, para clonar este repo).

## Despliegue

1.  **Clonar o descargar este repositorio** en tu máquina local.

2.  **Iniciar el entorno**:
    Ejecuta el siguiente comando en la raíz del proyecto. Esto descargará las imágenes, construirá las personalizadas (Hive y Trino con plugins de Ozone) e iniciará los servicios.

    ```bash
    docker-compose up -d --build
    ```

    *Nota:* la imagen de Trino compila en el build un JAR de `ozone-filesystem-hadoop3` sombreado con el prefijo de Protobuf que usa Trino (`io.trino.hadoop.$internal`). Esto evita el cuelgue de `CREATE TABLE` descrito en HDDS-12116. El primer build tarda porque descarga dependencias Maven.

3.  **Verificar el estado**:
    Asegúrate de que todos los contenedores estén en estado `running`.

    ```bash
    docker-compose ps
    ```

    *Nota: El servicio `init-ozone` es un contenedor efímero que se ejecuta una vez para crear el volumen y bucket iniciales y luego se detiene. Esto es normal.*

## Acceso a los Servicios

| Servicio | URL / Puerto | Descripción |
| :--- | :--- | :--- |
| **Trino UI** | [http://localhost:8080](http://localhost:8080) | Monitorización de queries y cluster Trino. Usuario: `admin`. |
| **Ozone OM UI** | [http://localhost:9874](http://localhost:9874) | Explorador de archivos y estado de Ozone. |
| **Trino JDBC** | `localhost:8080` | Puerto para conectar clientes SQL (DBeaver, DataGrip, CLI). |

## Guía de Uso Rápida

### Conectarse a Trino

Puedes usar cualquier cliente SQL compatible con Trino (como DBeaver) o la CLI de Trino.
*   **Host**: `localhost`
*   **Port**: `8080`
*   **User**: `admin` (sin contraseña)
*   **Catalog**: `iceberg`

### Ejemplo de Prueba (SQL)

Ejecuta el siguiente script SQL para verificar que todo funciona correctamente. Esto creará un esquema y una tabla Iceberg, insertará datos que se guardarán en Ozone y luego los consultará.

```sql
-- 1. Crear un esquema (namespace)
CREATE SCHEMA iceberg.demo_schema;

-- 2. Crear una tabla Iceberg
CREATE TABLE iceberg.demo_schema.usuarios (
    id INTEGER,
    nombre VARCHAR,
    fecha_registro DATE
);

-- 3. Insertar datos
INSERT INTO iceberg.demo_schema.usuarios VALUES 
(1, 'Ana', DATE '2023-01-15'),
(2, 'Carlos', DATE '2023-02-20'),
(3, 'Beatriz', DATE '2023-03-10');

-- 4. Consultar datos
SELECT * FROM iceberg.demo_schema.usuarios;
```

## Carga de datos de prueba (`load_fake_data.py`)

Script Python que genera datos sintéticos con Faker y los inserta en tablas Iceberg usando el conector de Trino.

* Qué hace:
    * Crea (si no existen) y trunca las tablas `customers`, `products` y `sales` en el catálogo `iceberg`, esquema `default`.
    * Inserta ~20k clientes, ~5k productos y ~50k ventas en lotes de 1000 filas.
* Requisitos previos: entorno levantado (`docker-compose up -d`), Python 3.9+ con dependencias `faker` y `trino` instaladas (por ejemplo `pip install faker trino` o `uv pip install faker trino`).
* Uso típico:

    ```bash
    # en la raíz del repo
    python load_fake_data.py
    # o con uv
    uv run python load_fake_data.py
    ```

El script se conecta a `localhost:8080` (Trino), usuario `python-loader`, catálogo `iceberg`, esquema `default`. Ajusta los contadores o el `BATCH_SIZE` dentro del archivo si necesitas más/menos datos.

### Verificar en Ozone

Después de insertar datos, puedes ir a la UI de Ozone ([http://localhost:9874](http://localhost:9874)) y navegar por el volumen `vol1` -> bucket `bucket1`. Deberías ver la estructura de carpetas creada por Iceberg (`warehouse/demo_schema.db/usuarios/...`).

## Comandos Útiles

*   **Detener el entorno**:
    ```bash
    docker-compose down
    ```

*   **Reiniciar forzando reconstrucción** (útil si cambias configuración):
    ```bash
    docker-compose up -d --build --force-recreate
    ```

*   **Ver logs de un servicio** (ej. Trino):
    ```bash
    docker-compose logs -f trino
    ```
