"""
Event repository.

Fetches pre-computed StructuredEvent rows from the events table.
Used as a fallback when /events/latest has no clusters (e.g. no messages
or all filtered out).
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import StructuredEvent

logger = logging.getLogger(__name__)


async def get_recent_events(
    session: AsyncSession,
    hours: int = 24,
    limit: int = 500,
) -> list[StructuredEvent]:
    """
    Return recent StructuredEvents from the last N hours.

    Used when clustering produces no events — ensures the feed still shows
    pre-computed events from developments_populate_job or analysis.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    stmt = (
        select(StructuredEvent)
        .where(StructuredEvent.created_at >= cutoff)
        .order_by(StructuredEvent.created_at.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows = list(result.scalars().all())
    logger.debug(
        "get_recent_events(hours=%d, limit=%d) → %d rows",
        hours,
        limit,
        len(rows),
    )
    return rows
