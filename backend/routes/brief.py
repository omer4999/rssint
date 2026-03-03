"""
Hourly intelligence brief endpoint.

GET /brief/hourly — return the pre-computed situation summary for the
current UTC hour, generating it on first request.
"""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from routes.deps import get_db
from schemas import HourlyBriefResponse
from services.hourly_brief_service import generate_hourly_brief

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/brief", tags=["Brief"])

DbSession = Annotated[AsyncSession, Depends(get_db)]


@router.get(
    "/hourly",
    response_model=HourlyBriefResponse,
    summary="Current-hour intelligence brief",
)
async def hourly_brief(db: DbSession) -> HourlyBriefResponse:
    brief = await generate_hourly_brief(db)
    return HourlyBriefResponse.model_validate(brief)
