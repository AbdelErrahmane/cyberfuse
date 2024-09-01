from fastapi import APIRouter, HTTPException
from core.hdfs_session import check_spark_hdfs_connection
from services.syslog_service import write_spark_syslog, read_spark_syslog

router = APIRouter(
    prefix="/syslog",
    tags=["syslog"],
    responses={404: {"description": "Not found"}},
)

@router.get("/status")
async def check_spark_hdfs():
    return check_spark_hdfs_connection()

@router.get("/write/{deltatable}")
async def write_syslog_deltatable(deltatable: str):
    return write_spark_syslog(deltatable, True, True)

@router.get("/read/{deltatable}")
async def read_syslog_deltatable(deltatable: str):
    return read_spark_syslog(deltatable)