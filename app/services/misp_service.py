from core.misp_session import get_misp_session
from dotenv import load_dotenv
import os
import json
from core.spark_session import get_spark_session
from pyspark.sql.functions import col , explode


load_dotenv()
url = os.getenv('MISP_URL')
authkey = os.getenv('MISP_AUTHKEY')
spark = get_spark_session()


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
        col("Feed.provider").alias("provider"),
        col("Feed.url").alias("url"),
        col("Feed.enabled").alias("enabled"),
        col("Feed.distribution").alias("distribution"),
        col("Feed.source_format").alias("source_format")
    )
    print('Processed DataFrame:')
    processed_df.show()

    return processed_df

def process_events(event):
    # Convert JSON response to PySpark DataFrame
    event_rdd = spark.sparkContext.parallelize([json.dumps(event)])
    event_df = spark.read.json(event_rdd)
    event_df = event_df.repartition(10)


    # Extract Event Details
    event_details = event_df.select(
        col("Event.id").alias("id"),
        col("Event.orgc_id").alias("orgc_id"),
        col("Event.org_id").alias("org_id"),
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
        col("Feed.id").alias("feed_id"),
        col("Feed.name").alias("feed_name"),
        col("Feed.url").alias("feed_url"),
        col("Feed.provider").alias("feed_provider"),
        col("Feed.source_format").alias("feed_source_format"),
        col("Feed.lookup_visible").alias("feed_lookup_visible"),
        col("Feed.event_uuids").alias("feed_event_uuids")
    )


    # Extract Org and Orgc Details
    org_details = event_df.select(
        col("Event.Org.id").alias("org_id"),
        col("Event.Org.name").alias("org_name"),
        col("Event.Org.uuid").alias("org_uuid"),
        col("Event.Org.local").alias("org_local")
    )

    orgc_details = event_df.select(
        col("Event.Orgc.id").alias("orgc_id"),
        col("Event.Orgc.name").alias("orgc_name"),
        col("Event.Orgc.uuid").alias("orgc_uuid"),
        col("Event.Orgc.local").alias("orgc_local")
    )

    # Extract Attributes
    attributes = event_df.select(
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
    print('Event Details:')
    event_details.show(truncate=True)

    print('Feed Details:')
    feed_details.show(truncate=True)

    print('Org Details:')
    org_details.show(truncate=True)

    print('Orgc Details:')
    orgc_details.show(truncate=False)

    print('Attributes:')
    attributes.show(truncate=False)

    return event_details, feed_details, org_details, orgc_details, attributes
