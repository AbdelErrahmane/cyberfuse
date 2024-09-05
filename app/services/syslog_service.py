from core.spark_singleton import SparkSingleton
from services.delta_table_service import read_spark_delta, write_spark_delta, save_or_merge_delta_table

spark = SparkSingleton.get_instance()

def write_spark_syslog(file_name: str, header: bool, infer_schema: bool):
    file_extension = file_name.split(".")[-1]
    if file_extension in ["csv","txt","json","xls","xlsx","parquet","orc","avro","xml","jsonl","delta"]:
        file_extension = "com.crealytics.spark.excel" if file_extension in ["xls","xlsx"] else file_extension
        df = spark.read.format(file_extension).option("header", header).option("inferSchema", infer_schema).load(f"hdfs://hadoop-namenode:8020/data/syslog/{file_name}")
        print(f"Dataframe schema: {df.schema}")
        df.show()
        columns = df.columns
        print(f"Columns: {columns}")
        df = df.repartition(6)
        message_status = save_or_merge_delta_table(df, "syslog", columns[0])
        # message_status = write_spark_delta(df, "syslog",columns[0])
        return message_status

    else:
        return {"status": "error", "details": "Unsupported file extension"}

def read_spark_syslog(table_path: str):
    try:
        message_status = read_spark_delta(table_path)
        return message_status
    except Exception as e:
        return {"status": "Error", "details": str(e)}


