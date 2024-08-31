from pyspark.sql import SparkSession

def get_spark_session(app_name="CyberfuseSparkApplication"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
        .getOrCreate()
    return spark
