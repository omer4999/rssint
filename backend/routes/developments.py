"""
Developments endpoint.

GET /developments  – return a directed graph of high-impact events from the
                     last 72 hours, with edges from event_relations (LLM-
                     classified relations).
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from models import EventRelation, StructuredEvent
from routes.deps import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/developments", tags=["Developments"])

DbSession = Annotated[AsyncSession, Depends(get_db)]

_WINDOW_HOURS: int = 72
_DISCONNECTED_REMOVE_HOURS: int = 3
_MIN_CONFIDENCE: float = 0.4
_VALID_IMPACT: frozenset[str] = frozenset({"medium", "high", "critical"})


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class DevEvent(BaseModel):
    id: uuid.UUID
    title: str
    impact_level: str
    confidence: float
    first_seen: datetime
    actors: list[str] = []


class DevEdge(BaseModel):
    source_event_id: uuid.UUID
    target_event_id: uuid.UUID
    relation_type: str


class DevelopmentsResponse(BaseModel):
    events: list[DevEvent] = Field(default_factory=list)
    edges: list[DevEdge] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=DevelopmentsResponse,
    summary="Conflict development graph",
    description=(
        "Returns a directed graph of high-impact events from the last 72 hours. "
        "Edges are LLM-classified relations (response, retaliation, escalation, etc.)."
    ),
)
async def get_developments(db: DbSession) -> DevelopmentsResponse:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_WINDOW_HOURS)

    result = await db.execute(
        select(StructuredEvent).where(
            or_(
                StructuredEvent.first_seen >= cutoff,
                and_(
                    StructuredEvent.first_seen.is_(None),
                    StructuredEvent.created_at >= cutoff,
                ),
            ),
            StructuredEvent.confidence >= _MIN_CONFIDENCE,
            StructuredEvent.impact_level.in_(_VALID_IMPACT),
        ).order_by(StructuredEvent.first_seen.asc().nulls_last())
    )
    events = list(result.scalars().all())

    event_ids = [e.id for e in events]
    if not event_ids:
        return DevelopmentsResponse(events=[], edges=[])

    result = await db.execute(
        select(EventRelation).where(
            EventRelation.source_event_id.in_(event_ids),
            EventRelation.target_event_id.in_(event_ids),
        )
    )
    relations = list(result.scalars().all())

    connected_ids = {r.source_event_id for r in relations} | {
        r.target_event_id for r in relations
    }
    disconnected_cutoff = datetime.now(timezone.utc) - timedelta(
        hours=_DISCONNECTED_REMOVE_HOURS
    )

    event_list = [
        DevEvent(
            id=ev.id,
            title=ev.title,
            impact_level=ev.impact_level,
            confidence=ev.confidence or 0.0,
            first_seen=ev.first_seen or ev.created_at,
            actors=ev.actors or [],
        )
        for ev in events
        if ev.id in connected_ids
        or (
            (ev.first_seen or ev.created_at) is not None
            and (ev.first_seen or ev.created_at) >= disconnected_cutoff
        )
    ]

    kept_ids = {ev.id for ev in event_list}
    filtered = [
        x for x in relations
        if x.source_event_id in kept_ids and x.target_event_id in kept_ids
    ]
    seen_pairs: dict[tuple[uuid.UUID, uuid.UUID], DevEdge] = {}
    for r in sorted(filtered, key=lambda x: -(x.confidence or 0.0)):
        a, b = r.source_event_id, r.target_event_id
        pair = (min(a, b), max(a, b))
        if pair in seen_pairs:
            continue
        seen_pairs[pair] = DevEdge(
            source_event_id=r.source_event_id,
            target_event_id=r.target_event_id,
            relation_type=r.relation_type,
        )
    edge_list = list(seen_pairs.values())

    logger.info(
        "GET /developments → %d events, %d edges (from %d relations).",
        len(event_list), len(edge_list), len(filtered),
    )
    return DevelopmentsResponse(events=event_list, edges=edge_list)
