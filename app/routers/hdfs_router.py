from fastapi import APIRouter, HTTPException

router = APIRouter(
    prefix="/hdfs",
    tags=["hdfs"],
    responses={404: {"description": "Not found"}},
)

@router.get("/status")
async def run_job():
    return True