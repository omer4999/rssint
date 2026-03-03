"""
30-minute background job: build development graph by classifying event relations.

Runs every 1800 seconds.  Fetches unprocessed high-impact events, uses LLM-only
reasoning (no cosine similarity or embeddings) for relation classification and
deduplication, and persists to event_relations.  Never blocks request handlers.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from openai import AsyncOpenAI
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from models import EventRelation, StructuredEvent
from services.development_dedup_service import classify_duplicate
from services.development_relation_service import classify_relation

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import async_sessionmaker

logger = logging.getLogger(__name__)

_INTERVAL_SECONDS: int = 1800
_WINDOW_HOURS: int = 72
_MIN_CONFIDENCE: float = 0.4
_DEDUP_CANDIDATE_LIMIT: int = 30
_RELATION_CANDIDATE_LIMIT: int = 100
_VALID_IMPACT: frozenset[str] = frozenset({"medium", "high", "critical"})


def _normalize_actor(a: str) -> str:
    """Normalize actor name for comparison (lowercase, stripped)."""
    return str(a).strip().lower() if a else ""


def _actors_overlap(ev_a: object, ev_b: object) -> bool:
    """True if the two events share at least one actor (case-insensitive)."""
    actors_a = set(
        _normalize_actor(a)
        for a in (getattr(ev_a, "actors", None) or [])
        if _normalize_actor(a)
    )
    actors_b = set(
        _normalize_actor(a)
        for a in (getattr(ev_b, "actors", None) or [])
        if _normalize_actor(a)
    )
    return bool(actors_a & actors_b)


async def build_development_graph(
    session_factory: "async_sessionmaker[AsyncSession]",
    openai_api_key: str,
) -> None:
    """
    One cycle: deduplicate, then process unprocessed events, classify relations, persist.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_WINDOW_HOURS)

    async with session_factory() as session:
        result = await session.execute(
            select(StructuredEvent).where(
                StructuredEvent.first_seen >= cutoff,
                StructuredEvent.confidence >= _MIN_CONFIDENCE,
                StructuredEvent.impact_level.in_(_VALID_IMPACT),
                StructuredEvent.development_processed == False,
            ).order_by(StructuredEvent.first_seen.asc())
        )
        new_events = list(result.scalars().all())

    if not new_events:
        logger.debug("Development graph: no unprocessed events.")
        return

    async with session_factory() as session:
        result = await session.execute(
            select(StructuredEvent).where(
                StructuredEvent.first_seen >= cutoff,
                StructuredEvent.confidence >= _MIN_CONFIDENCE,
                StructuredEvent.impact_level.in_(_VALID_IMPACT),
                StructuredEvent.development_processed == True,
            ).order_by(StructuredEvent.first_seen.asc())
        )
        existing_events = list(result.scalars().all())

    result = await session_factory().execute(
        select(StructuredEvent).where(
            StructuredEvent.first_seen >= cutoff,
            StructuredEvent.confidence >= _MIN_CONFIDENCE,
            StructuredEvent.impact_level.in_(_VALID_IMPACT),
        ).order_by(StructuredEvent.first_seen.asc())
    )
    all_events = list(result.scalars().all())

    client = AsyncOpenAI(api_key=openai_api_key or "")
    edges_created = 0
    duplicates_merged = 0

    async with session_factory() as session:
        for new_ev in new_events:
            dedup_candidates = sorted(
                existing_events,
                key=lambda e: e.first_seen or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )[: _DEDUP_CANDIDATE_LIMIT]

            merged = False
            if dedup_candidates:
                for existing in dedup_candidates:
                    is_dup = await classify_duplicate(new_ev, existing, client)
                    if is_dup:
                        new_count = (getattr(existing, "development_source_count", None) or 1) + 1
                        new_conf = max(
                            existing.confidence or 0.0,
                            new_ev.confidence or 0.0,
                        )
                        await session.execute(
                            update(StructuredEvent)
                            .where(StructuredEvent.id == existing.id)
                            .values(
                                development_source_count=new_count,
                                confidence=new_conf,
                            )
                        )
                        await session.execute(
                            update(StructuredEvent)
                            .where(StructuredEvent.id == new_ev.id)
                            .values(development_processed=True)
                        )
                        merged = True
                        duplicates_merged += 1
                        break

            if merged:
                continue

            raw_candidates = [
                e for e in all_events
                if e.first_seen and new_ev.first_seen
                and e.first_seen > new_ev.first_seen
            ]
            candidates = sorted(
                raw_candidates,
                key=lambda e: (
                    not _actors_overlap(new_ev, e),
                    e.first_seen,
                ),
            )[: _RELATION_CANDIDATE_LIMIT]

            for cand in candidates:
                relation = await classify_relation(new_ev, cand, client)
                if relation is None:
                    continue

                existing = await session.execute(
                    select(EventRelation).where(
                        EventRelation.source_event_id == cand.id,
                        EventRelation.target_event_id == new_ev.id,
                    )
                )
                if existing.scalar_one_or_none() is not None:
                    continue

                stmt = (
                    pg_insert(EventRelation)
                    .values(
                        source_event_id=new_ev.id,
                        target_event_id=cand.id,
                        relation_type=relation.relation_type,
                        confidence=relation.confidence,
                        reasoning=relation.reasoning,
                    )
                    .on_conflict_do_nothing(
                        index_elements=["source_event_id", "target_event_id"],
                    )
                )
                await session.execute(stmt)
                edges_created += 1

            await session.execute(
                update(StructuredEvent)
                .where(StructuredEvent.id == new_ev.id)
                .values(development_processed=True)
            )

        await session.commit()

    if duplicates_merged > 0:
        logger.info(
            "Development graph: merged %d duplicate(s) into existing nodes.",
            duplicates_merged,
        )
    if edges_created > 0:
        logger.info(
            "Development graph: processed %d events, created %d edges.",
            len(new_events), edges_created,
        )


async def run_development_graph_loop(
    session_factory: "async_sessionmaker[AsyncSession]",
    openai_api_key: str,
) -> None:
    """Run build_development_graph every 30 minutes."""
    logger.info(
        "Development graph job started (interval=%ds).",
        _INTERVAL_SECONDS,
    )
    while True:
        try:
            await build_development_graph(session_factory, openai_api_key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Development graph job error: %s", exc)
        await asyncio.sleep(_INTERVAL_SECONDS)
