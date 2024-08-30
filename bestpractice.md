# Best Practice Spark

## 1. Handle Big Data Efficiently

When working with big data, consider the following best practices:

- **Avoid Collecting Large DataFrames to Driver**: Instead of using `.collect()`, use `.toJSON().collect()` to get a JSON representation of the data directly from the workers, reducing memory usage on the driver.
- **Use Partitions Wisely**: Leverage Spark's partitioning capabilities. If you know your data is going to be filtered or grouped by a specific column frequently, consider repartitioning or bucketing by that column to improve performance.
- **Persist and Cache DataFrames**: When repeatedly accessing a DataFrame, cache or persist it in memory to avoid recomputation, but be mindful of memory usage.
- **Limit Data for JSON Responses**: For endpoints returning JSON, limit the data size to avoid overwhelming the client or the network. You might want to paginate results or summarize the data instead of returning all rows.
- **Optimize Data Reading and Writing**: Use appropriate data formats (like Parquet or ORC) that support efficient reading and writing and have built-in compression.

## 2. Best Practices for Managing Big Data in FastAPI with PySpark

- **Data Sampling for Large Datasets**: When the dataset is too large to return in full, consider returning a sample or a summary (e.g., descriptive statistics, counts, etc.) instead of the entire dataset.
- **Lazy Execution and Optimization**: Take advantage of Spark's lazy execution model. Avoid triggering actions (like `.collect()`, `.show()`, `.toPandas()`) unnecessarily and ensure that transformations are pipelined effectively.
- **Efficient Memory Management**:
  - Use `broadcast()` to send small lookup tables to all worker nodes when performing joins.
  - Use `checkpoint()` or `persist()` methods to save intermediate results that are expensive to recompute.
- **Pagination for Large Responses**: If you need to return large datasets, implement pagination. Send a limited number of rows at a time and provide a mechanism for the client to request more data as needed.
- **Asynchronous Job Handling**: For long-running Spark jobs, use asynchronous processing or background tasks in FastAPI. This prevents blocking the main thread and improves the responsiveness of your API. You can use libraries like `celery` or FastAPI’s built-in background tasks.

## 3. Optimize Data Serialization

- **Optimize Data Serialization**: Use formats like Apache Arrow when converting between PySpark and Pandas DataFrames, which provides efficient in-memory columnar storage.

## Summary

1. **Set up your FastAPI application to use PySpark** by initializing a Spark session and creating endpoints that utilize Spark's capabilities.
2. **Handle big data efficiently** by leveraging Spark's distributed computing capabilities and optimizing your use of DataFrames, partitions, caching, and serialization.
3. **Return JSON responses smartly** by summarizing or sampling large datasets and using efficient serialization formats.
4. **Consider asynchronous job handling** for long-running tasks to keep your API responsive.

By following these steps and best practices, you can create a robust FastAPI application that effectively integrates with PySpark and handles big data workloads efficiently.


## Apache Hive
Apache Hive is not a database itself but a data warehouse infrastructure built on top of Hadoop. It provides tools to enable easy data summarization, ad-hoc querying, and analysis of large datasets stored in Hadoop-compatible file systems. Hive uses a SQL-like language called HiveQL for querying data, which gets translated into MapReduce jobs executed on the Hadoop cluster.

### Key Components of Hive:
- **Metastore**: Stores metadata about the tables, columns, partitions, and the data types. This metadata is typically stored in a relational database like MySQL, PostgreSQL, or another supported RDBMS.
- **HiveQL**: A SQL-like query language used to interact with the data stored in Hive.
Driver: Manages the lifecycle of a HiveQL statement as it moves through Hive.
- **Compiler**: Compiles HiveQL into a directed acyclic graph of MapReduce jobs.
Execution Engine: Executes the tasks produced by the compiler in proper dependency order.
Example Use Case:
- **Data Storage**: Store large datasets in Hadoop Distributed File System (HDFS).
Data Processing: Use Hive to run SQL-like queries on the data stored in HDFS.
- **Metadata Management**: Use a relational database (like MySQL or PostgreSQL) to store metadata about the data in Hive.

### Hive and spark interaction:
you can have interaction between Apache Hive and Apache Spark. Spark can read from and write to Hive tables, allowing you to leverage Hive's data warehousing capabilities alongside Spark's powerful data processing engine. This integration is commonly used in big data applications to perform complex queries and transformations on large datasets.

Steps to Integrate Hive with Spark:
- Configure Hive Metastore: Ensure that the Hive Metastore is properly configured and accessible by Spark.
- Spark Session Configuration: Configure the Spark session to use Hive support.
- Read from Hive: Use Spark to read data from Hive tables.
- Write to Hive: Use Spark to write data back to Hive tables.

add 
```python
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .enableHiveSupport() \

```

add that at `master.env` file this configuration:
```configuration
# Hive configuration
SPARK_CONF_spark.sql.warehouse.dir=/user/hive/warehouse
SPARK_CONF_spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083
SPARK_CONF_spark.sql.catalogImplementation=hive
```


add that at `worker.env` file this configuration:
```configuration
# Hive configuration
SPARK_CONF_spark.sql.warehouse.dir=/user/hive/warehouse
SPARK_CONF_spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083
SPARK_CONF_spark.sql.catalogImplementation=hive
```

and add an image of  hive ``hive:2.3.2-postgresql-metastore`` and add a volume to storage the data in ``docker-compose.yml``