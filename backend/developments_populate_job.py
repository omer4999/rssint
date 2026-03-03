"""
Background job: populate StructuredEvents for the developments 72-hour window.

Runs every 6 hours. Fetches messages from the last 72 hours, clusters them,
and creates/updates StructuredEvent rows via the LLM service. This ensures
the developments graph has events across the full 72-hour window, not just
from recent /events/latest or hourly brief requests (which use 30–60 min).
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from repositories import message_repository as repo
from services.event_clustering_service import cluster_messages

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

_INTERVAL_SECONDS: int = 6 * 3600  # 6 hours
_WINDOW_MINUTES: int = 72 * 60  # 72 hours


async def populate_developments_events(
    session_factory: "async_sessionmaker[AsyncSession]",
) -> None:
    """
    Fetch messages from the last 72 hours, cluster them, and create
    StructuredEvent rows. The LLM service persists via cluster_messages.
    """
    async with session_factory() as session:
        raw_messages = await repo.get_recent_messages(
            session, minutes=_WINDOW_MINUTES
        )

    if not raw_messages:
        logger.debug("Developments populate: no messages in 72h window.")
        return

    clusters = await cluster_messages(
        messages=raw_messages,
        window_minutes=_WINDOW_MINUTES,
    )

    logger.info(
        "Developments populate: %d messages → %d clusters (72h window).",
        len(raw_messages),
        len(clusters),
    )


async def run_developments_populate_loop(
    session_factory: "async_sessionmaker[AsyncSession]",
) -> None:
    """Run populate_developments_events every 6 hours."""
    logger.info(
        "Developments populate job started (interval=%ds, window=%dh).",
        _INTERVAL_SECONDS,
        _WINDOW_MINUTES // 60,
    )
    while True:
        try:
            await populate_developments_events(session_factory)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Developments populate error: %s", exc)
        await asyncio.sleep(_INTERVAL_SECONDS)
