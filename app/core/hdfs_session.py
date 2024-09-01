from core.spark_singleton import SparkSingleton

spark = SparkSingleton.get_instance()

def check_spark_hdfs_connection():
    try:
        df = spark.read.csv("hdfs://hadoop-namenode:8020/data/syslog/Syslog-servers-sample.csv", header=True, inferSchema=True)
        count = df.count()
        return {"status": "connected", "details": f"Read {count} rows from HDFS"}
    except Exception as e:
        return {"status": "error", "details": str(e)}