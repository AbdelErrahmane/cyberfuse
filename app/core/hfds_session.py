from core.spark_session import get_spark_session


def check_spark_hdfs_connection():
    try:
        spark = get_spark_session()
        df = spark.read.csv("hdfs://hadoop-namenode:8020/data/syslog/Syslog-servers-sample.csv", header=True, inferSchema=True)
        count = df.count()
        return {"status": "connected", "details": f"Read {count} rows from HDFS"}
    except Exception as e:
        return {"status": "error", "details": str(e)}