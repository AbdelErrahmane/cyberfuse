from fastapi import APIRouter, HTTPException
from services.sentinel_service import start_eventhub_stream

router = APIRouter(
    prefix="/sentinel",
    tags=["sentinel"],
    responses={404: {"description": "Not found"}},
)


@router.post("/startstream/{timestamp}")
def start_stream(timestamp: str):
    try:
        start_eventhub_stream(timestamp)
        return {"status": "Event Hub Stream Started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
