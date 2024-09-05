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
- **Lazy Execution and Optimization**: Take advantage of Spark's lazy execution model. Avoid triggering actions (like `.collect()`, `.show()`, `.toPandas()`) unnecessarily and ensure that transformations are pipelined effectively.s
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

#  Delta Lake

## Delta Lake

**Purpose**: Delta Lake is an open-source storage layer that brings ACID (Atomicity, Consistency, Isolation, Durability) transactions to Apache Spark and big data workloads. It enables building reliable data lakes.

**Storage**: Delta Lake stores data in Parquet format on cloud storage systems like AWS S3, Azure Data Lake Storage (ADLS), or HDFS.

**Schema Management**: Delta Lake supports schema enforcement and schema evolution, allowing for more flexible data management.

**Query Language**: Delta Lake can be queried using Spark SQL.

**Data Processing**: Delta Lake leverages Apache Spark for data processing and provides features like time travel (data versioning), upserts, and deletes.

**Use Cases**: Real-time data processing, streaming data, data lakes, and scenarios requiring ACID transactions.

## Key Differences

### ACID Transactions

- **Delta Lake**: Provides full ACID transaction support, ensuring data reliability and consistency.

### Data Format

- **Delta Lake**: Uses Parquet format with additional transaction logs for ACID compliance.

### Schema Management

- **Delta Lake**: Supports schema enforcement and evolution, allowing for more flexible schema management.

### Performance

- **Delta Lake**: Optimized for performance with features like data skipping, Z-order indexing, and caching.

### Use Cases

- **Delta Lake**: Ideal for real-time data processing, streaming, and scenarios requiring ACID transactions.

## HDFS:

- **Namenode:** Manages metadata and namespace operations, critical for the functioning of HDFS.
- **Datanode:** Stores actual data, performs read/write operations, and reports to the Namenode.

## Singleton Pattern Explanation
The Singleton pattern is a design pattern that ensures a class has only one instance and provides a global point of access to that instance. This is useful when exactly one object is needed to coordinate actions across the system.

### Key Characteristics of Singleton Pattern

- **Single Instance:** Only one instance of the class is created.
- **Global Access:** The single instance is globally accessible.
- **Lazy Initialization:** The instance is created only when it is needed.
