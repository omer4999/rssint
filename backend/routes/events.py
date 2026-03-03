"""
Events endpoints.

GET /events/latest  – fetch recent messages, cluster them with OpenAI
                      embeddings, and return ranked EventCluster objects.

Post-processing includes a title-based deduplication pass that collapses
events whose normalised titles are identical — a safety net for clusters
that slipped past the embedding-level dedup in the LLM service.
"""

import asyncio
import logging
from typing import Annotated

import numpy as np
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from repositories import message_repository as repo
from routes.deps import get_db
from schemas import EventCard, EventClusterResponse, EventsResponse
from services.event_clustering_service import EventCluster, cluster_messages

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["Events"])

DbSession = Annotated[AsyncSession, Depends(get_db)]

_RESPONSE_DEDUP_THRESHOLD: float = 0.78


# ---------------------------------------------------------------------------
# Schema conversion helper
# ---------------------------------------------------------------------------


def _cluster_to_response(cluster: EventCluster) -> EventClusterResponse:
    """Map an ``EventCluster`` dataclass onto the Pydantic response schema."""
    return EventClusterResponse(
        event_id=cluster.event_id,
        title=cluster.title,
        summary=cluster.summary,
        event_type=cluster.event_type,
        actors=cluster.actors,
        locations=cluster.locations,
        impact_level=cluster.impact_level,
        confidence=round(cluster.confidence, 4),
        source_count=cluster.source_count,
        channels=cluster.channels,
        first_seen=cluster.first_seen,
        last_seen=cluster.last_seen,
        messages=[
            EventCard(
                channel=m.channel,
                timestamp=m.timestamp,
                text=m.text,
                url=m.url,
            )
            for m in cluster.messages
        ],
    )


def _dedup_responses(
    clusters: list[EventCluster],
    vecs: np.ndarray,
) -> list[EventCluster]:
    """
    Collapse clusters whose titles are semantically near-identical.

    Uses titles (not full summaries) because titles are short, focused
    headlines that better capture the core event identity.  Two different
    LLM-generated summaries about the same strike will diverge in detail
    but their titles will stay close.

    Keeps the cluster with the highest source_count (then highest confidence)
    from each group of duplicates.  Merged clusters contribute their sources,
    channels, actors, and locations to the survivor.
    """
    if len(clusters) < 2:
        return clusters

    merged_into: dict[int, int] = {}

    for i in range(len(clusters)):
        if i in merged_into:
            continue
        for j in range(i + 1, len(clusters)):
            if j in merged_into:
                continue
            if clusters[i].event_type != clusters[j].event_type:
                continue
            sim = float(np.dot(vecs[i], vecs[j]))
            if sim >= _RESPONSE_DEDUP_THRESHOLD:
                survivor, victim = (i, j) if (
                    clusters[i].source_count >= clusters[j].source_count
                ) else (j, i)
                merged_into[victim] = survivor

                s = clusters[survivor]
                v = clusters[victim]
                s.channels = list(set(s.channels) | set(v.channels))
                s.source_count = len(s.channels)
                s.confidence = max(s.confidence, v.confidence)
                s.actors = list(set(s.actors) | set(v.actors))
                s.locations = list(set(s.locations) | set(v.locations))
                s.messages = s.messages + [
                    m for m in v.messages
                    if m.text not in {em.text for em in s.messages}
                ]
                if v.first_seen < s.first_seen:
                    s.first_seen = v.first_seen
                if v.last_seen > s.last_seen:
                    s.last_seen = v.last_seen

                logger.info(
                    "Response dedup: '%s' merged into '%s' (title_sim=%.3f).",
                    v.title[:50], s.title[:50], sim,
                )

    result = [c for idx, c in enumerate(clusters) if idx not in merged_into]
    if len(result) < len(clusters):
        logger.info(
            "Response dedup removed %d duplicate(s) from %d clusters.",
            len(clusters) - len(result), len(clusters),
        )
    return result


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get(
    "/latest",
    response_model=EventsResponse,
    summary="Latest clustered intelligence events",
    description=(
        "Fetch messages from the last N minutes, run embedding-based semantic "
        "clustering, and return ranked EventCluster objects. "
        "Clusters are sorted by source_count DESC then last_seen DESC."
    ),
)
async def latest_events(
    db: DbSession,
    minutes: int = Query(
        default=30,
        ge=1,
        le=1440,
        description="Look-back window in minutes (default 30, max 1440 = 24 h)",
    ),
    limit: int = Query(
        default=500,
        ge=1,
        le=500,
        description="Maximum number of event clusters to return (default 500 = all)",
    ),
) -> EventsResponse:
    from services.embedding_service import embed_texts_sync

    raw_messages = await repo.get_recent_messages(db, minutes=minutes)

    clusters = await cluster_messages(
        messages=raw_messages,
        window_minutes=minutes,
    )

    # Post-processing dedup: collapse semantically identical events that
    # slipped past the DB-level dedup (e.g. pre-existing duplicates or
    # clusters generated before the lock was added).
    if len(clusters) >= 2:
        titles = [c.title for c in clusters]
        vecs = np.array(
            embed_texts_sync(titles),
            dtype=np.float32,
        )
        if vecs.size > 0:
            loop = asyncio.get_event_loop()
            clusters = await loop.run_in_executor(
                None, _dedup_responses, clusters, vecs,
            )

    top_clusters = clusters[:limit]
    response_clusters = [_cluster_to_response(c) for c in top_clusters]

    logger.info(
        "GET /events/latest minutes=%d limit=%d "
        "→ %d raw messages, %d total clusters, %d returned",
        minutes,
        limit,
        len(raw_messages),
        len(clusters),
        len(response_clusters),
    )

    return EventsResponse(
        total=len(response_clusters),
        window_minutes=minutes,
        events=response_clusters,
    )
