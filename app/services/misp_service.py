from core.misp_session import get_misp_session
from dotenv import load_dotenv
import os
import json
from core.spark_singleton import SparkSingleton
from pyspark.sql.functions import col , explode
from services.delta_table_service import save_or_merge_delta_table
from pyspark.storagelevel import StorageLevel
import time
# from models.misp_models import Event, Feed, Org


load_dotenv()
url = os.getenv('MISP_URL')
authkey = os.getenv('MISP_AUTHKEY')
spark = SparkSingleton.get_instance()


def check_misp_connexion():
    session = get_misp_session(url,authkey)
    if "session" in session:
        return {"connexion": "Successful"}
    else:
        raise ValueError("MISP session not found")

async def get_json_session(uri):
    session = get_misp_session(url,authkey)
    if "session" in session:
        response = session['session'].get(f"{url}/{uri}", verify=False)
        if response.status_code == 200:
            print("Request successful!")
            return response.json()
        else:
            print(f"Request failed with status code: {response.status_code}")
        pass
    else:
        raise ValueError("MISP session not found")


def process_feeds(feeds):
    # Convert JSON response to PySpark DataFrame
    feeds_rdd = spark.sparkContext.parallelize([json.dumps(feeds)])
    feeds_df = spark.read.json(feeds_rdd)
    processed_df = feeds_df.select(
        col("Feed.id").alias("id"),
        col("Feed.name").alias("name"),
        col("Feed.url").alias("url"),
        col("Feed.provider").alias("provider"),
        col("Feed.source_format").alias("source_format"),
        col("Feed.enabled").alias("enabled"),
        col("Feed.distribution").alias("distribution")
    )
    print('Processed DataFrame:')
    processed_df.show()
    

    return processed_df

def process_events(event):
    # Convert JSON response to PySpark DataFrame
    event_rdd = spark.sparkContext.parallelize([json.dumps(event)])
    event_df = spark.read.json(event_rdd)
    if event_df.rdd.getNumPartitions() > 10:
        event_df = event_df.coalesce(10)
    event_df = event_df.persist(StorageLevel.MEMORY_AND_DISK)
    # Extract Event Details
    event_details = event_df.select(
        col("Event.id").alias("id"),
        col("Event.orgc_id").alias("orgc_id"),
        col("Event.org_id").alias("org_id"),
        col("Event.Feed.id").alias("feed_id"),
        col("Event.date").alias("date"),
        col("Event.threat_level_id").alias("threat_level_id"),
        col("Event.info").alias("info"),
        col("Event.published").alias("published"),
        col("Event.uuid").alias("uuid"),
        col("Event.attribute_count").alias("attribute_count"),
        col("Event.analysis").alias("analysis"),
        col("Event.timestamp").alias("timestamp"),
        col("Event.distribution").alias("distribution"),
        col("Event.proposal_email_lock").alias("proposal_email_lock"),
        col("Event.locked").alias("locked"),
        col("Event.publish_timestamp").alias("publish_timestamp"),
        col("Event.sharing_group_id").alias("sharing_group_id"),
        col("Event.disable_correlation").alias("disable_correlation"),
        col("Event.extends_uuid").alias("extends_uuid"),
        col("Event.protected").alias("protected"),
        col("Event.event_creator_email").alias("event_creator_email")
    )

    # Extract Feed Details
    feed_details = event_df.select(
        explode(col("Event.Feed")).alias("Feed")
    ).select(
        col("Feed.id").alias("id"),
        col("Feed.name").alias("name"),
        col("Feed.url").alias("url"),
        col("Feed.provider").alias("provider"),
        col("Feed.source_format").alias("source_format"),
        col("Feed.lookup_visible").alias("lookup_visible"),
        col("Feed.event_uuids").alias("event_uuids")
    )


    # Extract Org and Orgc Details
    org_details = event_df.select(
        col("Event.Org.id").alias("id"),
        col("Event.Org.name").alias("name"),
        col("Event.Org.uuid").alias("uuid"),
        col("Event.Org.local").alias("local")
    )

    orgc_details = event_df.select(
        col("Event.Orgc.id").alias("id"),
        col("Event.Orgc.name").alias("name"),
        col("Event.Orgc.uuid").alias("uuid"),
        col("Event.Orgc.local").alias("local")
    )

    # Extract Attributes
    attributes_details = event_df.select(
       explode(col("Event.Attribute")).alias("Attribute")
    ).select(
        col("Attribute.id").alias("attribute_id"),
        col("Attribute.type").alias("attribute_type"),
        col("Attribute.category").alias("attribute_category"),
        col("Attribute.to_ids").alias("attribute_to_ids"),
        col("Attribute.uuid").alias("attribute_uuid"),
        col("Attribute.event_id").alias("attribute_event_id"),
        col("Attribute.distribution").alias("attribute_distribution"),
        col("Attribute.timestamp").alias("attribute_timestamp"),
        col("Attribute.comment").alias("attribute_comment"),
        col("Attribute.sharing_group_id").alias("attribute_sharing_group_id"),
        col("Attribute.deleted").alias("attribute_deleted"),
        col("Attribute.disable_correlation").alias("attribute_disable_correlation"),
        col("Attribute.object_id").alias("attribute_object_id"),
        col("Attribute.object_relation").alias("attribute_object_relation"),
        col("Attribute.first_seen").alias("attribute_first_seen"),
        col("Attribute.last_seen").alias("attribute_last_seen"),
        col("Attribute.value").alias("attribute_value")
    )


    # Show DataFrames
    print('==========Event Details:===============')
    event_details.show()

    print('==============Feed Details:==============')
    feed_details.show()

    print('==============Org Details:==============')

    print('==============Orgc Details:==============')

    print('==============Attributes Details:==============')

    attributes_details = attributes_details.repartition(4)

    message_status_event = save_or_merge_delta_table(event_details, "misp/events", event_details.columns[0])
    # message_status_feed = save_or_merge_delta_table(feed_details, "misp/feeds", feed_details.columns[0])
    message_status_org = save_or_merge_delta_table(org_details, "misp/orgs", org_details.columns[0])
    message_status_orgc = save_or_merge_delta_table(orgc_details, "misp/orgcs", orgc_details.columns[0])
    message_status_attributes = save_or_merge_delta_table(attributes_details, "misp/attributes", attributes_details.columns[0])

    # message_status_event = ""
    message_status_feed = " Keep the feed table as it is"
    # message_status_org = ""
    # message_status_orgc = ""
    # message_status_attributes = ""

    return message_status_event, message_status_feed, message_status_org, message_status_orgc, message_status_attributes
