"""
Hourly intelligence brief — one LLM-generated situation summary per UTC hour.

The brief is generated once, stored in ``hourly_briefs``, and served to all
users until the hour rolls over.  If a brief already exists for the current
hour window it is returned immediately without touching the LLM.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import HourlyBrief

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_MAX_INPUT_CHARS: int = 2_000

_PROMPT = """\
You are an intelligence desk editor.

Based on the following event briefs from the past hour, \
produce a concise 3–5 sentence situation summary.

Rules:
- Neutral tone
- No speculation
- No source mentions
- No repetition
- Group related developments
- Plain text only
- Always respond in English

Event briefs:
{event_summaries}"""

_FALLBACK_EMPTY = (
    "No significant multi-source events were recorded in the past hour."
)
_FALLBACK_ERROR = "Summary temporarily unavailable."


def _floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


async def generate_hourly_brief(
    db: AsyncSession,
) -> HourlyBrief:
    """
    Return the hourly brief for the current UTC hour.

    If none exists yet, generate one from the latest event clusters.
    """
    now = datetime.now(timezone.utc)
    window_start = _floor_hour(now)
    window_end = window_start + timedelta(hours=1)

    existing = await db.execute(
        select(HourlyBrief).where(HourlyBrief.window_start == window_start)
    )
    row = existing.scalar_one_or_none()
    if row is not None:
        return row

    t0 = time.monotonic()

    summary_text = await _build_summary(db, minutes=60)

    brief = HourlyBrief(
        window_start=window_start,
        window_end=window_end,
        summary=summary_text,
    )
    db.add(brief)
    await db.commit()
    await db.refresh(brief)

    elapsed = time.monotonic() - t0
    logger.info("Hourly brief generated in %.1fs for window %s.", elapsed, window_start)

    return brief


async def _build_summary(db: AsyncSession, minutes: int) -> str:
    """Collect cluster summaries and synthesise via LLM."""
    from repositories import message_repository as repo
    from services import llm_service

    raw_messages = await repo.get_recent_messages(db, minutes=minutes)
    if not raw_messages:
        return _FALLBACK_EMPTY

    from services.event_clustering_service import cluster_messages

    clusters = await cluster_messages(
        messages=raw_messages,
        window_minutes=minutes,
    )

    event_summaries = [c.summary for c in clusters if c.summary and c.confidence >= 0.4]
    if not event_summaries:
        return _FALLBACK_EMPTY

    combined = "\n".join(f"- {s}" for s in event_summaries)
    if len(combined) > _MAX_INPUT_CHARS:
        combined = combined[:_MAX_INPUT_CHARS].rsplit("\n", 1)[0]

    if not llm_service._API_KEY:
        return combined

    try:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=llm_service._API_KEY)
        prompt = _PROMPT.format(event_summaries=combined)

        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.2,
        )
        text = response.choices[0].message.content.strip()
        logger.info(
            "Hourly brief LLM call: %d chars, tokens=%s.",
            len(text),
            getattr(response.usage, "total_tokens", "?"),
        )
        return text
    except Exception as exc:
        logger.warning("Hourly brief LLM call failed: %s", exc)
        return _FALLBACK_ERROR
