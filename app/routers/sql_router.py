from fastapi import APIRouter, HTTPException
from core.spark_singleton import SparkSingleton

router = APIRouter(
    prefix="/sql",
    tags=["sql"],
    responses={404: {"description": "Not found"}},
)

@router.get("/query/")
async def run_sql_query(query: str):
    try:
        spark = SparkSingleton.get_instance()
        df = spark.sql(query)
        result = df.collect()
        return {"status": "Success", "data": [row.asDict() for row in result]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
