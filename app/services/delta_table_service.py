from delta.tables import DeltaTable
from core.spark_singleton import SparkSingleton


spark = SparkSingleton.get_instance()

def clean_column_names(df):
    for col_name in df.columns:
        # Replace spaces, brackets, and other characters with underscores
        clean_name = col_name.replace(" ", "_").replace("[", "").replace("]", "").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "")
        df = df.withColumnRenamed(col_name, clean_name)
    return df
    

def write_spark_delta(dataframe, table_path: str, partition_column: str,overwrite_schema=False):
    try:
        dataframe = clean_column_names(dataframe)
        dataframe.write.format("delta").mode("overwrite")\
        .option("overwriteSchema", overwrite_schema)\
        .save(f"hdfs://hadoop-namenode:8020/delta/{table_path}")
        return {"status": "Success", "details": "Data saved in Delta table in HDFS"}
    except Exception as e:
        return {"status": "Error", "details": str(e)}

def read_spark_delta(table_path: str):
    try:
        delta_table = DeltaTable.forPath(spark, f"hdfs://hadoop-namenode:8020/delta/{table_path}")
        delta_df = delta_table.toDF()
        delta_df.show()
        return {"status": "Success", "details": "Delta table read from Datatable in HDFS"}
    except Exception as e:
        return {"status": "Error", "details": str(e)}


def save_or_merge_delta_table(df, delta_path, partition_column):
    delta_path = f"hdfs://hadoop-namenode:8020/delta/{delta_path}"
    # Check if the Delta table exists
    print(f"Saving or merging data into Delta table {delta_path} with partition column {partition_column}")

    df = clean_column_names(df)
    
    if DeltaTable.isDeltaTable(spark, delta_path):
        print(f"Delta table {delta_path} exists")
        deltaTable = DeltaTable.forPath(spark, delta_path)
        # Check if the Delta table is empty
        if deltaTable.toDF().count() > 0:
            # If the table is not empty, perform the merge operation
            print(f"Merging data into Delta table {delta_path}")
            df = df.dropDuplicates([partition_column])
            deltaTable.alias("target").merge(
                df.alias("source"),
                f"target.{partition_column} = source.{partition_column}"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            return {"status": "Success", "details": "Data merged into Delta table in HDFS"}

        else:
            # If the table is empty, append the data
            print(f"Appending data to Delta table {delta_path}")
            df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(delta_path)
            return {"status": "Success", "details": "Data appended to empty dataframe in Delta table in HDFS"}
    else:
        # If the Delta table does not exist, write the DataFrame as a new Delta table
        print(f"Creating Delta table {delta_path}")
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(delta_path)
        if  df.is_cached:
            df = df.unpersist()
        return {"status": "Success", "details": "Data saved in Delta table in HDFS"}
