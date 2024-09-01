from pyspark.sql import SparkSession

class SparkSingleton:
    _instance = None

    @staticmethod
    def get_instance(app_name="CyberfuseSparkApplication"):
        if SparkSingleton._instance is None:
            SparkSingleton._instance = SparkSession.builder \
                .appName(app_name) \
                .master("spark://spark-master:7077") \
                .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        return SparkSingleton._instance