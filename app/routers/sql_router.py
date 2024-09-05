from fastapi import APIRouter, HTTPException,Query
from core.spark_singleton import SparkSingleton
import json
from decimal import Decimal
from datetime import datetime, date

router = APIRouter(
    prefix="/sql",
    tags=["sql"],
    responses={404: {"description": "Not found"}},
)

def serialize_row(row):
    """
    Helper function to serialize PySpark Row objects to a JSON-friendly format.
    """
    row_dict = row.asDict(recursive=True)

    for key, value in row_dict.items():
        if isinstance(value, (datetime, date)):
            row_dict[key] = value.isoformat()  # Convert datetime objects to string
        elif isinstance(value, Decimal):
            row_dict[key] = float(value)  # Convert Decimal to float
        elif isinstance(value, bytes):
            row_dict[key] = value.decode('utf-8')  # Convert bytes to string
        elif isinstance(value, dict):
            try:
                row_dict[key] = json.dumps(value)  # Serialize nested dictionaries
            except Exception as e:
                row_dict[key] = str(value)  # Fallback to string
        else:
            print("else.........") # Log unknown types for debugging
            try:
                json.dumps(value)  # Check if value is serializable
            except TypeError:
                print(f"Non-serializable type found: {key} = {type(value)}, value = {value}")
                row_dict[key] = str(value)  # Convert to string as a last resort

    return row_dict



@router.get("/query/")
async def run_sql_query(query: str):
    try:
        spark = SparkSingleton.get_instance()
        df = spark.sql(query)
        result = df.collect()
        return {"status": "Success", "data": [row.asDict() for row in result]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/query/sentinel/pagination/")
async def run_sql_query(query: str, page: int = Query(1, gt=0), page_size: int = Query(100, gt=0, le=1000)):
    try:
        spark = SparkSingleton.get_instance()
        offset = (page - 1) * page_size
        paginated_query = f"{query} LIMIT {page_size} OFFSET {offset}"
        df = spark.sql(paginated_query)
        result = df.collect()
        serialized_result = [serialize_row(row) for row in result]
        return {"status": "Success", "data": serialized_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))