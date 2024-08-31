from core.spark_session import get_spark_session


def read_spark_hdfs(file_name: str, header: bool, infer_schema: bool):
    spark = get_spark_session()
    file_extension = file_name.split(".")[-1]
    if file_extension in ["csv","txt","json","xls","xlsx","parquet","orc","avro","xml","jsonl","delta"]:
        file_extension = "com.crealytics.spark.excel" if file_extension in ["xls","xlsx"] else file_extension
        df = spark.read.format(file_extension).option("header", header).option("inferSchema", infer_schema).load(f"hdfs://hadoop-namenode:8020/data/syslog/{file_name}")
        print(f"Dataframe schema: {df.schema}")
        df.show()
        df_json = df.toJSON().collect()
        return df_json
    else:
        return {"status": "error", "details": "Unsupported file extension"}
