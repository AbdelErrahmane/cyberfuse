from pyspark.sql import SparkSession

class SparkSingleton:
    _instance = None

    @staticmethod
    def get_instance(app_name="CyberfuseSparkApplication"):
        if SparkSingleton._instance is None:
            packages = [
                "com.crealytics:spark-excel_2.12:0.13.5",
                "io.delta:delta-spark_2.12:3.2.0",
                "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21"
            ]
            
            SparkSingleton._instance = SparkSession.builder \
                .appName(app_name) \
                .master("spark://spark-master:7077") \
                .config("spark.jars.packages", ",".join(packages)) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        return SparkSingleton._instance