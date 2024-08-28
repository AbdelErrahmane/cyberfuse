from fastapi import FastAPI
from pyspark.sql import SparkSession

app = FastAPI()

# Initialize Spark session and connect to the Spark master
spark = SparkSession.builder \
    .appName("FastAPI with PySpark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

@app.get("/")
def read_root():
    sc = spark.sparkContext
    # Get information about the Spark context
    app_id = sc.applicationId
    master_url = sc.master
    version = sc.version
    app_name = sc.appName
    default_parallelism = sc.defaultParallelism
    default_min_partitions = sc.defaultMinPartitions
    
    # Convert JavaObject to Python dictionary
    executor_memory_status = sc._jsc.sc().getExecutorMemoryStatus().toString()
    num_workers = executor_memory_status.count('(') - 1  # Subtract 1 to exclude the driver
    
    return {
        "message": "Welcome to the FastAPI with PySpark application!",
        "app_id": app_id,
        "master_url": master_url,
        "version": version,
        "app_name": app_name,
        "default_parallelism": default_parallelism,
        "default_min_partitions": default_min_partitions,
        "num_workers": num_workers
    }


@app.get("/data")
def get_data():
    # Example PySpark code to create a DataFrame
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    
    # Convert DataFrame to JSON
    result = df.toJSON().collect()
    return {"data": result}

@app.get("/count")
def get_count():
    # Example PySpark code to count the number of rows in a DataFrame
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    
    count = df.count()
    return {"count": count}