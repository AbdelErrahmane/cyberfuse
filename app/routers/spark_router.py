from fastapi import APIRouter, HTTPException
from services.spark_service import get_spark_info

router = APIRouter(
    prefix="/spark",
    tags=["spark"],
    responses={404: {"description": "Not found"}},
)

@router.get("/status")
async def run_job():
    try:
        status = get_spark_info()
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))