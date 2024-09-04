from fastapi import APIRouter, HTTPException
from services.sentinel_service import start_eventhub_stream
from services.delta_table_service import read_spark_delta

router = APIRouter(
    prefix="/sentinel",
    tags=["sentinel"],
    responses={404: {"description": "Not found"}},
)


@router.post("/startstream/{timestamp}")
def start_stream(timestamp: int):
    try:
        start_eventhub_stream(timestamp)
        return {"status": f"Event Hub Stream Finished after {timestamp} seconds"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/read/{deltatable}")
async def read_spark_sentinel(deltatable: str):
    deltatable_new = f"sentinel/{deltatable}"
    return read_spark_delta(deltatable_new)
