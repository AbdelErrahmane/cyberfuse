from fastapi import FastAPI
from pyspark.sql import SparkSession
from routers import  spark_router, misp_router, hdfs_router


app = FastAPI()

app.include_router(spark_router.router)
app.include_router(misp_router.router)
app.include_router(hdfs_router.router)