"""Path and cycle search endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.search import CycleSearchRequest, PathSearchRequest, SearchResponse
from app.services import pathfinder

router = APIRouter(prefix="/search", tags=["search"])


@router.post("/paths", response_model=SearchResponse)
async def post_search_paths(
    body: PathSearchRequest,
    db: AsyncSession = Depends(get_db),
):
    return await pathfinder.find_paths(db, body)


@router.post("/cycles", response_model=SearchResponse)
async def post_search_cycles(
    body: CycleSearchRequest,
    db: AsyncSession = Depends(get_db),
):
    return await pathfinder.find_cycles(db, body)
