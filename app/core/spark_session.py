from pyspark.sql import SparkSession

def get_spark_session(app_name="CyberfuseSparkApplication"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    return spark
