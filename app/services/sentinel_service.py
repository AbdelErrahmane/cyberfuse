from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType, MapType
import os
import time
from core.spark_singleton import SparkSingleton


spark = SparkSingleton.get_instance()

def start_eventhub_stream(timestamp):
    # Azure Event Hub Configurations
    connectionString = os.getenv("EVENTHUB_CONNECTION_STRING")
    consumerGroup = os.getenv("EVENTHUB_CONSUMER_GROUP")
    print(f"Connection String: {connectionString}")
    print(f"Consumer Group: {consumerGroup}")
    
    ehConf = {
        'eventhubs.connectionString': connectionString,
        'eventhubs.consumerGroup': consumerGroup,
    }
    # Optional: Encrypt the connection string
    try:
        ehConf['eventhubs.connectionString'] = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
    except Exception as e:
        print(f"Error during encryption: {e}")
        raise

    raw_file_schema = StructType([
        StructField('body', StringType(), True),
        StructField('partition', StringType(), True),
        StructField('offset', StringType(), True),
        StructField('sequenceNumber', StringType(), True),
        StructField('enqueuedTime', TimestampType(), True),
        StructField('publisher', StringType(), True),
        StructField('partitionKey', StringType(), True),
        StructField('properties', MapType(StringType(), StringType(), True)),
        StructField('systemProperties', MapType(StringType(), StringType(), True))
    ])

    def _explode_columns(df):
        owner_schema = StructType([
                                    StructField("objectId", StringType(), True),
                                    StructField("email", StringType(), True),
                                    StructField("assignedTo", StringType(), True),
                                    StructField("userPrincipalName", StringType(), True)
                                    ])
        json_schema = StructType([
        StructField("records", ArrayType(
            StructType([
                StructField("AdditionalData", StructType([
                    StructField("alertsCount", IntegerType(), True),
                    StructField("bookmarksCount", IntegerType(), True),
                    StructField("commentsCount", IntegerType(), True),
                    StructField("alertProductNames", ArrayType(StringType()), True),
                    StructField("tactics", ArrayType(StringType()), True),
                    StructField("techniques", ArrayType(StringType()), True),
                ]), True),
                StructField("AlertIds", ArrayType(StringType()), True),
                StructField("BookmarkIds", ArrayType(StringType()), True),
                StructField("Comments", ArrayType(StringType()), True),
                StructField("CreatedTime", TimestampType(), True),
                StructField("Description", StringType(), True),
                StructField("FirstActivityTime", TimestampType(), True),
                StructField("IncidentName", StringType(), True),
                StructField("IncidentNumber", IntegerType(), True),
                StructField("IncidentUrl", StringType(), True),
                StructField("Labels", ArrayType(StringType()), True),
                StructField("LastActivityTime", TimestampType(), True),
                StructField("LastModifiedTime", TimestampType(), True),
                StructField("ModifiedBy", StringType(), True),
                StructField("Owner", owner_schema, True),
                StructField("ProviderIncidentId", StringType(), True),
                StructField("ProviderName", StringType(), True),
                StructField("RelatedAnalyticRuleIds", ArrayType(StringType()), True),
                StructField("Severity", StringType(), True),
                StructField("SourceSystem", StringType(), True),
                StructField("Status", StringType(), True),
                StructField("Tasks", ArrayType(StringType()), True),
                StructField("TenantId", StringType(), True),
                StructField("TimeGenerated", TimestampType(), True),
                StructField("Title", StringType(), True),
                StructField("Type", StringType(), True),
                StructField("_Internal_WorkspaceResourceId", StringType(), True),
                ])
            ))
        ])
        df = df.select('*', from_json(col("body").cast("string"), json_schema).alias("Payload"))
        df = df.select('*', 'Payload.*').drop('Payload')
        df = df.select('*', explode(col('records')).alias('recordsStruct')).drop('records')
        df = df.select('*', 'recordsStruct.*').drop('recordsStruct')
        return df

    # Reading from Event Hub
    eventHubDF = spark.readStream \
        .format("eventhubs") \
        .options(**ehConf) \
        .schema(raw_file_schema) \
        .load() \
        .transform(_explode_columns)

    def process_batch(df, batch_id):
        distinct_types = df.select("Type").distinct().collect()
        for row in distinct_types:
            types = row["Type"]
            table_name = types.replace(" ", "_")
            batch_df = df.filter(col("Type") == types)
            delta_path = f"hdfs://hadoop-namenode:8020/delta/sentinel/{table_name}"
            batch_df.write.format("delta").mode("append").save(delta_path)

    query = eventHubDF.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/path/to/checkpoint") \
        .start()

    time.sleep(timestamp)  # Or manage stream processing logic
    query.stop()