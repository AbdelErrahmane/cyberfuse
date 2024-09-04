from fastapi import FastAPI
from pyspark.sql import SparkSession
from routers import  spark_router, misp_router, syslog_router, sentinel_router, sql_router


app = FastAPI()

app.include_router(spark_router.router)
app.include_router(misp_router.router)
app.include_router(syslog_router.router)
app.include_router(sentinel_router.router)
app.include_router(sql_router.router)