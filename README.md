# Documentation of Cyberfuse Project
## Overview (Spark)
Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python, and R, and an optimized engine that supports general execution graphs. Spark is known for its speed, ease of use, and sophisticated analytics.

### Spark Components
1. **Spark Master**: The master node in a Spark cluster that manages the cluster resources and schedules the execution of applications.
2. **Spark Worker**: The worker nodes in a Spark cluster that execute tasks assigned by the master.
3. **Spark Driver**: The process that runs the main() function of the application and creates the SparkContext.
4. **Spark Executor**: The distributed agents responsible for executing tasks on the worker nodes.
### Spark sangleton Class
create a single SparkSession instance that can be used across your entire application, you can follow these steps:
- **Create a Singleton Class for SparkSession:** This ensures that only one instance of SparkSession is created and shared across the application.
- **Use the Singleton Class in Your Modules:** Import and use the singleton instance wherever needed.
## Overview (MISP)
This project is a FastAPI application integrated with PySpark and MISP (Malware Information Sharing Platform). It provides endpoints to interact with MISP data and Spark cluster information.

## Table of Contents
1. [Setup Instructions](#setup-instructions)
2. [Environment Variables](#environment-variables)
3. [Docker Setup](#docker-setup)
4. [API Endpoints](#api-endpoints)
5. [Services](#services)
6. [Configuration Files](#configuration-files)

## Setup Instructions

### Prerequisites
- Docker
- Docker Compose

### Installation
1. Clone the repository:
    ```sh
    git clone https://github.com/AbdelErrahmane/cyberfuse.git
    cd cyberfuse
    ```

2. Create a `.env` file in the root directory with the following content:
    ```properties
    MISP_URL = "https://your-misp-url"
    MISP_AUTHKEY = "your-misp-authkey"
    ```

3. Build and run the Docker containers:
    ```sh
    docker-compose up --build
    ```

## Environment Variables
- **MISP_URL**: The URL of the MISP instance.
- **MISP_AUTHKEY**: The authentication key for the MISP instance.

## Docker Setup
The project uses Docker Compose to set up the following services:
- **spark-master**: The Spark master node.
- **spark-worker**: The Spark worker node.
- **fastapi**: The FastAPI application.

### Docker Compose Configuration
The `docker-compose.yml` file defines the services and their configurations.

### Dockerfile
The `Dockerfile` sets up the FastAPI application environment.

## API Endpoints

### Spark Router
- **GET /spark/status**: Returns the status of the Spark cluster.

### MISP Router
- **GET /misp/status**: Checks the connection to the MISP instance.
- **GET /misp/feeds**: Fetches and processes MISP feeds.
- **GET /misp/feeds/read**: Reads the MISP feeds Delta table.
- **GET /misp/events/view/{event_id}**: Fetches and processes a specific MISP event by ID.

### Sentinel Router
- **POST /sentinel/startstream/{timestamp}**: Starts the Event Hub stream for a specified duration.
- **GET /sentinel/read/{deltatable}**: Reads a specified Delta table from the Sentinel data.

### Syslog Router
- **GET /syslog/status**: Checks the connection to Spark and HDFS.
- **GET /syslog/write/{path_file}**: Writes syslog data to a Delta table.
- **GET /syslog/read/{deltatable}**: Reads a specified Delta table from the syslog data.

### SQL Router
- **GET /sql/query**: Runs a SQL query on the Spark cluster.

## Services

### Spark Service
- **get_spark_info**: Retrieves information about the Spark cluster.

### MISP Service
- **check_misp_connexion**: Checks the connection to the MISP instance.
- **get_json_session**: Retrieves JSON data from the MISP instance.
- **process_feeds**: Processes MISP feeds into a PySpark DataFrame.
- **process_events**: Processes MISP events into a PySpark DataFrame.

### Sentinel Service
- **start_eventhub_stream**: Starts the Event Hub stream for a specified duration and processes the data.

### Syslog Service
- **write_spark_syslog**: Writes syslog data to a Delta table.
- **read_spark_syslog**: Reads a specified Delta table from the syslog data.

### Delta Table Service
- **clean_column_names**: Cleans column names by replacing spaces and special characters with underscores.
- **write_spark_delta**: Writes a DataFrame to a Delta table in HDFS.
- **read_spark_delta**: Reads a Delta table from HDFS.
- **save_or_merge_delta_table**: Saves or merges data into a Delta table in HDFS.

## Configuration Files

### `requirements.txt`
Lists the Python dependencies for the project:
```plaintext
fastapi>=0.95.0
uvicorn>=0.22.0
pyspark==3.5.2
requests
urllib3
python-dotenv
delta-spark==3.2.0
```


### `worker.env`
Configuration for the Spark worker:
```properties
SPARK_MODE=worker
SPARK_MASTER=spark://spark-master:7077
SPARK_WORKER_CORES=3
SPARK_WORKER_MEMORY=3G
SPARK_WORKER_PORT=7078
SPARK_WORKER_WEBUI_PORT=8081
# New configuration settings
SPARK_CONF_spark_serializer=org.apache.spark.serializer.KryoSerializer
SPARK_CONF_spark_kryoserializer_buffer_max=2000m
SPARK_CONF_spark_driver_maxResultSize=2g
SPARK_CONF_spark_rpc_message_maxSize=2000
SPARK_CONF_spark_task_maxFailures=10
SPARK_CONF_spark_executor_memory=4g
SPARK_CONF_spark_driver_memory=6g
# cluser Node
CLUSTER_NAME=cyberfuse-hadoop-cluster
# Hive configuration
# SPARK_CONF_spark.sql.warehouse.dir=/user/hive/warehouse
# SPARK_CONF_spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083
# SPARK_CONF_spark.sql.catalogImplementation=hive
```

### `master.env`
Configuration for the Spark master:

```properties
SPARK_MODE=master
SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080
# New configuration settings
SPARK_CONF_spark_serializer=org.apache.spark.serializer.KryoSerializer
SPARK_CONF_spark_kryoserializer_buffer_max=2000m
SPARK_CONF_spark_driver_maxResultSize=2g
SPARK_CONF_spark_rpc_message_maxSize=2000
SPARK_CONF_spark_task_maxFailures=10
SPARK_CONF_spark_executor_memory=4g
SPARK_CONF_spark_driver_memory=6g
# HDFS Configuration
SPARK_CONF_spark_hadoop_fs_defaultFS=hdfs://hadoop-namenode:8020
CLUSTER_NAME=cyberfuse-hadoop-cluster

```