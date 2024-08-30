from core.spark_session import get_spark_session

def get_spark_info():
    spark = get_spark_session()
    sc = spark.sparkContext
    app_id = sc.applicationId
    master_url = sc.master
    version = sc.version
    app_name = sc.appName
    ui_url = sc.uiWebUrl
    default_parallelism = sc.defaultParallelism
    default_min_partitions = sc.defaultMinPartitions
    executor_memory_status = sc._jsc.sc().getExecutorMemoryStatus().toString()
    num_workers = executor_memory_status.count('(') - 1  
    
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