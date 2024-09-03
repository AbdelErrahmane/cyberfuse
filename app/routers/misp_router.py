from fastapi import APIRouter, HTTPException
from services.misp_service import check_misp_connexion, get_json_session,process_feeds,process_events
from services.delta_table_service import write_spark_delta, read_spark_delta,save_or_merge_delta_table
import logging
from pyspark.storagelevel import StorageLevel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/misp",
    tags=["misp"],
    responses={404: {"description": "Not found"}},
)

@router.get("/status")
async def run_job():
    try:
        status = check_misp_connexion()
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/feeds")
async def get_feeds_write_delta_table():
    try:
        feeds = await get_json_session("feeds")
        if feeds:
            feeds_df = process_feeds(feeds)
            columns = feeds_df.columns
            feeds_df = feeds_df.repartition(4)
            feeds_df = feeds_df.persist(StorageLevel.MEMORY_AND_DISK)
            message_status = save_or_merge_delta_table(feeds_df, "misp/feeds", columns[0])
            return message_status
        else:
           raise HTTPException(status_code=500, detail="Failed to fetch feeds")
    except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@router.get("/feeds/read/")
async def read_feeds_deltatable():
    return read_spark_delta("misp/feeds")

@router.get("/events/view/{event_id}")
async def get_event(event_id: int):
    uri = f"events/view/{event_id}"
    try:
        event = await get_json_session(uri)
        if event:
            event_details, feed_details, org_details, orgc_details, attributes = process_events(event)
            return {
                    "event_details": event_details,
                    "feed_details": feed_details,
                    "org_details": org_details,
                    "orgc_details": orgc_details,
                    "attributes": attributes
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to fetch events")
    except Exception as e:
        logger.error(f"Error fetching event {event_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))