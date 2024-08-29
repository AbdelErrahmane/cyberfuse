from fastapi import APIRouter, HTTPException
from services.misp_service import check_misp_connexion, get_feeds

router = APIRouter(
    prefix="/misp",
    tags=["misp"],
    responses={404: {"description": "Not found"}},
)

@router.get("/status")
async def run_job():
    try:
        status = check_misp_connexion()
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/feeds")
async def get_fetch():
    # try:
        response = await get_feeds()
        # print(status)
        return response
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))