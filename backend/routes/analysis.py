"""
Analysis endpoint.

GET /analysis – return the cached conflict analysis (generated every 6 hours by
               a background job). All users receive the same analysis.
"""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import ConflictAnalysis
from routes.deps import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analysis", tags=["Analysis"])

DbSession = Annotated[AsyncSession, Depends(get_db)]


# ---------------------------------------------------------------------------
# Response schema
# ---------------------------------------------------------------------------


class AnalysisResponse(BaseModel):
    conflict_overview: str = ""
    latest_developments: str = ""
    possible_outcomes: str = ""
    generated_at: str = ""


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=AnalysisResponse,
    summary="Get cached conflict analysis",
    description=(
        "Returns the most recent conflict analysis. Generated every 6 hours by "
        "a background job. All users receive the same cached analysis."
    ),
)
async def get_analysis(db: DbSession) -> AnalysisResponse:
    result = await db.execute(
        select(ConflictAnalysis)
        .order_by(desc(ConflictAnalysis.generated_at))
        .limit(1)
    )
    row = result.scalar_one_or_none()

    if row is None:
        return AnalysisResponse(
            conflict_overview="No analysis available yet. The first analysis will be generated within 6 hours of startup.",
            latest_developments="",
            possible_outcomes="",
            generated_at="",
        )

    return AnalysisResponse(
        conflict_overview=row.conflict_overview or "",
        latest_developments=row.latest_developments or "",
        possible_outcomes=row.possible_outcomes or "",
        generated_at=row.generated_at.isoformat() if row.generated_at else "",
    )
