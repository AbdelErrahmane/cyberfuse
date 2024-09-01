from core.spark_singleton import SparkSingleton

spark = SparkSingleton.get_instance()

def get_spark_info():
    sc = spark.sparkContext
    app_id = sc.applicationId
    master_url = sc.master
    version = sc.version
    app_name = sc.appName
    ui_url = sc.uiWebUrl
    default_parallelism = sc.defaultParallelism
    default_min_partitions = sc.defaultMinPartitions
    executor_memory_status = sc._jsc.sc().getExecutorMemoryStatus().toString()
    num_workers = executor_memory_status.count('(')
    
    return {
        "message": "Welcome to the FastAPI with PySpark application!",
        "app_id": app_id,
        "master_url": master_url,
        "version": version,
        "app_name": app_name,
        "default_parallelism": default_parallelism,
        "default_min_partitions": default_min_partitions,
        "uiWebUrl": ui_url,
        "num_workers": num_workers
    }