from fastapi import APIRouter, HTTPException
from core.hdfs_session import check_spark_hdfs_connection
from services.hdfs_service import read_spark_hdfs

router = APIRouter(
    prefix="/hdfs",
    tags=["hdfs"],
    responses={404: {"description": "Not found"}},
)

@router.get("/status")
async def run_job():
    return check_spark_hdfs_connection()

@router.get("/statuss")
async def run_job():
    return read_spark_hdfs("Cisco-ASA-Syslog-sample.xlsx", True, True)