from fastapi import APIRouter, HTTPException
from services.misp_service import check_misp_connexion, get_json_session,process_feeds,process_events

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
async def get_fetch():
    try:
        feeds = await get_json_session("feeds")
        if feeds:
            feeds_df = process_feeds(feeds)
            return feeds_df.toJSON().collect()
        else:
           raise HTTPException(status_code=500, detail="Failed to fetch feeds")
    except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@router.get("/events/view/{event_id}")
async def get_event(event_id: int):
    uri = f"events/view/{event_id}"
    try:
        event = await get_json_session(uri)
        if event:
            event_details, feed_details, org_details, orgc_details, attributes = process_events(event)
            return {
                    "event_details": event_details.toJSON().collect(),
                    "feed_details": feed_details.toJSON().collect(),
                    "org_details": org_details.toJSON().collect(),
                    "orgc_details": orgc_details.toJSON().collect(),
                    "attributes": attributes.toJSON().collect()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to fetch events")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))