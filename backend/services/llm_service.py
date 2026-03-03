"""
LLM service — structured intelligence event analysis via OpenAI.

Produces a full structured event object (title, summary, event_type, actors,
locations, impact_level) for every cluster and persists it in the ``events``
table keyed by a deterministic content hash.

Event-level deduplication
-------------------------
Before inserting a new event, the service generates an embedding of the
summary using the OpenAI embeddings API and queries the ``events`` table
for semantically similar events created in the last 12 hours.  If cosine
similarity >= 0.88, the existing event is updated (last_seen, confidence,
actors, locations) instead of creating a duplicate row.

A ``_DEDUP_LOCK`` serialises the check-and-insert path so concurrent
background tasks cannot race past each other.

Caching
-------
L1 – in-memory dict  (cluster_hash → JSON string).
L2 – ``events`` PostgreSQL table (survives restarts).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import numpy as np
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from models import StructuredEvent

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import async_sessionmaker

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state (set once via init)
# ---------------------------------------------------------------------------

_SESSION_FACTORY: "async_sessionmaker[AsyncSession] | None" = None
_API_KEY: str = ""

_CACHE: dict[str, str] = {}  # cluster_hash → JSON string
_PENDING: set[str] = set()
_MERGED_HASHES: set[str] = set()  # cluster_hashes folded into another event
_DEDUP_LOCK: asyncio.Lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEDUP_SIM_THRESHOLD: float = 0.78
_DEDUP_WINDOW_HOURS: int = 12

VALID_EVENT_TYPES = frozenset({
    "military_strike", "rocket_attack", "terror_attack", "explosion",
    "armed_clash", "political_statement", "diplomatic_move", "sanctions",
    "protest", "internal_unrest", "infrastructure_damage", "cyber_attack",
    "unknown",
})
VALID_IMPACT_LEVELS = frozenset({"low", "medium", "high", "critical"})


@dataclass
class AnalysisResult:
    """In-memory representation returned to the clustering service."""
    title: str = ""
    summary: str = ""
    event_type: str = "unknown"
    actors: list[str] = field(default_factory=list)
    locations: list[str] = field(default_factory=list)
    impact_level: str = "low"


_MAX_REPRESENTATIVE: int = 3
_MAX_INPUT_CHARS: int = 1_200
_MAX_CONCURRENT: int = 5
_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(_MAX_CONCURRENT)

_URL_RE: re.Pattern[str] = re.compile(r"https?://\S+", re.IGNORECASE)
_MARKDOWN_RE: re.Pattern[str] = re.compile(r"[*_`~#>|\\]")

_PROMPT_TEMPLATE = """\
You are an intelligence analyst system.

Your task is to analyze a cluster of related news messages and produce a structured intelligence event object.

STRICT RULES:

1. Output must be valid JSON only.
2. Do not include markdown.
3. Do not include explanations.
4. Do not include extra fields.
5. All output must be in English.
6. Ignore reporter names, channel names, promotional text, emojis, URLs, formatting symbols.
7. Remove redundant phrases.
8. Produce a neutral, factual tone.
9. Summary must be 2–3 concise sentences maximum.
10. Title must be a short headline of max 15 words.

ALLOWED EVENT TYPES (choose exactly one):
- military_strike
- rocket_attack
- terror_attack
- explosion
- armed_clash
- political_statement
- diplomatic_move
- sanctions
- protest
- internal_unrest
- infrastructure_damage
- cyber_attack
- unknown

ALLOWED IMPACT LEVELS (choose exactly one):
- low: limited tactical or rhetorical event
- medium: sustained military or political development
- high: cross-border escalation, significant damage, major response
- critical: strategic shift, leadership targeting, mass casualties, or regional escalation

Return exactly this JSON structure:

{{"title": "...", "summary": "...", "event_type": "...", "actors": ["..."], "locations": ["..."], "impact_level": "..."}}

Now analyze the following messages:

{reports}"""

# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


def init(
    factory: "async_sessionmaker[AsyncSession]",
    api_key: str,
) -> None:
    global _SESSION_FACTORY, _API_KEY  # noqa: PLW0603
    _SESSION_FACTORY = factory
    _API_KEY = (api_key or "").strip()
    logger.info(
        "LLM service initialised (api_key=%s, db=%s).",
        "SET" if _API_KEY else "MISSING",
        "YES" if _SESSION_FACTORY else "NO",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean(text: str) -> str:
    text = _URL_RE.sub("", text)
    text = _MARKDOWN_RE.sub("", text)
    return " ".join(text.split())


def _select_representative(messages: list[str]) -> list[str]:
    selected: list[str] = []
    total = 0
    for raw in messages[:_MAX_REPRESENTATIVE]:
        cleaned = _clean(raw)
        if not cleaned:
            continue
        if total + len(cleaned) > _MAX_INPUT_CHARS:
            remaining = _MAX_INPUT_CHARS - total
            if remaining > 60:
                selected.append(cleaned[:remaining].rstrip())
            break
        selected.append(cleaned)
        total += len(cleaned)
    return selected


def compute_content_hash(representative: list[str]) -> str:
    canonical = "|".join(sorted(representative))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _parse_llm_json(raw: str) -> AnalysisResult:
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return AnalysisResult(summary=raw)

    title = str(data.get("title", "")).strip()
    summary = str(data.get("summary", "")).strip()
    event_type = str(data.get("event_type", "unknown")).strip().lower()
    impact_level = str(data.get("impact_level", "low")).strip().lower()

    raw_actors = data.get("actors", [])
    actors = [str(a).strip() for a in raw_actors if str(a).strip()] if isinstance(raw_actors, list) else []

    raw_locs = data.get("locations", [])
    locations = [str(loc).strip() for loc in raw_locs if str(loc).strip()] if isinstance(raw_locs, list) else []

    if event_type not in VALID_EVENT_TYPES:
        event_type = "unknown"
    if impact_level not in VALID_IMPACT_LEVELS:
        impact_level = "low"
    if not title:
        title = (summary[:100].rsplit(" ", 1)[0] + "…") if len(summary) > 100 else summary

    return AnalysisResult(
        title=title, summary=summary, event_type=event_type,
        actors=actors, locations=locations, impact_level=impact_level,
    )


def _serialize(result: AnalysisResult) -> str:
    return json.dumps({
        "title": result.title, "summary": result.summary,
        "event_type": result.event_type, "actors": result.actors,
        "locations": result.locations, "impact_level": result.impact_level,
    }, ensure_ascii=False)


def _deserialize(cached: str) -> AnalysisResult:
    try:
        data = json.loads(cached)
        if isinstance(data, dict) and "event_type" in data:
            return _parse_llm_json(cached)
    except (json.JSONDecodeError, TypeError):
        pass
    parts = cached.split("\n", 1)
    if len(parts) == 2:
        return AnalysisResult(title=parts[0], summary=parts[1])
    return AnalysisResult(summary=cached)


def _encode_text(text: str) -> list[float] | None:
    """Generate embedding from text using OpenAI embeddings API."""
    from services.embedding_service import embed_texts_sync

    if not text:
        return None
    result = embed_texts_sync([text])
    return result[0] if result else None


# ---------------------------------------------------------------------------
# DB persistence — with embedding-based deduplication
# ---------------------------------------------------------------------------


async def _db_get(cluster_hash: str) -> str | None:
    if _SESSION_FACTORY is None:
        return None
    try:
        async with _SESSION_FACTORY() as session:
            result = await session.execute(
                select(StructuredEvent).where(
                    StructuredEvent.cluster_hash == cluster_hash
                )
            )
            row = result.scalar_one_or_none()
            if row is None:
                return None
            return json.dumps({
                "title": row.title, "summary": row.summary,
                "event_type": row.event_type,
                "actors": row.actors or [], "locations": row.locations or [],
                "impact_level": row.impact_level,
            }, ensure_ascii=False)
    except Exception as exc:
        logger.debug("DB lookup failed (hash=%s…): %s", cluster_hash[:12], exc)
        return None


async def _find_similar_event(
    session: AsyncSession,
    embedding: list[float],
    event_type: str = "unknown",
) -> StructuredEvent | None:
    """Find a recent event with cosine similarity >= threshold (Python-side).

    Only candidates sharing the same ``event_type`` are considered so that
    a lower similarity threshold won't accidentally merge different kinds
    of events (e.g. a strike and a political statement).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_DEDUP_WINDOW_HOURS)

    result = await session.execute(
        select(StructuredEvent).where(
            StructuredEvent.embedding.isnot(None),
            StructuredEvent.created_at >= cutoff,
        )
    )
    candidates = result.scalars().all()
    if not candidates:
        return None

    new_vec = np.array(embedding, dtype=np.float32)
    new_norm = np.linalg.norm(new_vec)
    if new_norm == 0:
        return None

    best_event: StructuredEvent | None = None
    best_sim: float = -1.0

    for event in candidates:
        if event.event_type != event_type:
            continue
        stored = event.embedding
        if not stored or not isinstance(stored, list):
            continue
        cand_vec = np.array(stored, dtype=np.float32)
        cand_norm = np.linalg.norm(cand_vec)
        if cand_norm == 0:
            continue
        sim = float(np.dot(new_vec, cand_vec) / (new_norm * cand_norm))
        if sim >= _DEDUP_SIM_THRESHOLD and sim > best_sim:
            best_sim = sim
            best_event = event

    if best_event is not None:
        logger.info(
            "Dedup candidate: event=%s type=%s sim=%.4f (threshold=%.2f)",
            best_event.id, event_type, best_sim, _DEDUP_SIM_THRESHOLD,
        )
    return best_event


async def _db_put(
    cluster_hash: str,
    analysis: AnalysisResult,
    *,
    confidence: float | None = None,
    first_seen: Any = None,
    last_seen: Any = None,
    window_minutes: int | None = None,
) -> bool:
    """
    Persist an analysis with embedding-based deduplication.

    Returns ``True`` if this cluster was merged into an existing event
    (i.e. it is a duplicate and should not be shown separately).
    """
    if _SESSION_FACTORY is None:
        return False

    loop = asyncio.get_event_loop()
    dedup_text = f"{analysis.title} — {analysis.summary}"
    embedding = await loop.run_in_executor(None, _encode_text, dedup_text)

    async with _DEDUP_LOCK:
        try:
            async with _SESSION_FACTORY() as session:
                async with session.begin():
                    if embedding is not None:
                        existing = await _find_similar_event(
                            session, embedding, event_type=analysis.event_type,
                        )
                        if existing is not None:
                            merged_actors = list(set(
                                (existing.actors or []) + analysis.actors
                            ))
                            merged_locations = list(set(
                                (existing.locations or []) + analysis.locations
                            ))
                            new_confidence = max(
                                existing.confidence or 0.0,
                                confidence or 0.0,
                            )
                            new_last_seen = last_seen
                            if existing.last_seen and last_seen:
                                new_last_seen = max(existing.last_seen, last_seen)
                            elif existing.last_seen:
                                new_last_seen = existing.last_seen

                            await session.execute(
                                update(StructuredEvent)
                                .where(StructuredEvent.id == existing.id)
                                .values(
                                    last_seen=new_last_seen,
                                    confidence=new_confidence,
                                    actors=merged_actors,
                                    locations=merged_locations,
                                )
                            )
                            logger.info(
                                "Dedup merge: hash %s… → existing event %s "
                                "(conf %.2f→%.2f, actors %d→%d, locs %d→%d).",
                                cluster_hash[:12], existing.id,
                                existing.confidence or 0, new_confidence,
                                len(existing.actors or []), len(merged_actors),
                                len(existing.locations or []), len(merged_locations),
                            )
                            return True

                    from sqlalchemy.dialects.postgresql import insert as pg_insert

                    values: dict[str, Any] = {
                        "cluster_hash": cluster_hash,
                        "title": analysis.title,
                        "summary": analysis.summary,
                        "event_type": analysis.event_type,
                        "impact_level": analysis.impact_level,
                        "actors": analysis.actors,
                        "locations": analysis.locations,
                        "confidence": confidence,
                        "first_seen": first_seen,
                        "last_seen": last_seen,
                        "window_minutes": window_minutes,
                        "embedding": embedding,
                    }
                    stmt = (
                        pg_insert(StructuredEvent)
                        .values(**values)
                        .on_conflict_do_nothing(index_elements=["cluster_hash"])
                    )
                    await session.execute(stmt)
                    return False
        except Exception as exc:
            logger.debug("DB write failed (hash=%s…): %s", cluster_hash[:12], exc)
            return False


# ---------------------------------------------------------------------------
# OpenAI call
# ---------------------------------------------------------------------------


async def _call_openai(representative: list[str]) -> AnalysisResult:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=_API_KEY)
    prompt = _PROMPT_TEMPLATE.format(
        reports="\n".join(f"- {m}" for m in representative)
    )
    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500,
        temperature=0,
    )
    raw = response.choices[0].message.content.strip()
    logger.info(
        "OpenAI response (%d chars, tokens=%s).",
        len(raw),
        getattr(response.usage, "total_tokens", "?"),
    )
    return _parse_llm_json(raw)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def _generate_and_cache(
    cluster_hash: str,
    representative: list[str],
    *,
    confidence: float | None = None,
    first_seen: Any = None,
    last_seen: Any = None,
    window_minutes: int | None = None,
) -> None:
    """Background task: call OpenAI, cache in L1 + persist to events table."""
    if not _API_KEY:
        analysis = AnalysisResult(
            title=representative[0][:100],
            summary=representative[0],
        )
    else:
        async with _SEMAPHORE:
            try:
                analysis = await _call_openai(representative)
            except Exception as exc:
                logger.warning("OpenAI call failed: %s — text fallback.", exc)
                analysis = AnalysisResult(
                    title=representative[0][:100],
                    summary=" ".join(representative),
                )

    was_merged = await _db_put(
        cluster_hash, analysis,
        confidence=confidence,
        first_seen=first_seen,
        last_seen=last_seen,
        window_minutes=window_minutes,
    )

    if was_merged:
        _MERGED_HASHES.add(cluster_hash)
        logger.info("Hash %s… marked as merged duplicate.", cluster_hash[:12])
    else:
        _CACHE[cluster_hash] = _serialize(analysis)


async def get_or_generate_analysis(
    msg_texts: list[str],
    *,
    confidence: float | None = None,
    first_seen: Any = None,
    last_seen: Any = None,
    window_minutes: int | None = None,
) -> AnalysisResult | None:
    """
    Return ``AnalysisResult`` if cached, else schedule background generation
    and return ``None``.

    Returns ``None`` when:
    - The cluster is still being generated (background task running).
    - The cluster was merged into another event (duplicate).
    """
    if not msg_texts:
        return None

    representative = _select_representative(msg_texts)
    if not representative:
        return None

    cluster_hash = compute_content_hash(representative)

    # Duplicate — this hash was merged into an existing event.
    if cluster_hash in _MERGED_HASHES:
        return None

    # L1: in-memory
    if cluster_hash in _CACHE:
        return _deserialize(_CACHE[cluster_hash])

    # L2: events table
    cached = await _db_get(cluster_hash)
    if cached:
        _CACHE[cluster_hash] = cached
        return _deserialize(cached)

    # Not cached — fire background task, return None (not ready).
    if cluster_hash not in _PENDING:
        _PENDING.add(cluster_hash)

        async def _bg() -> None:
            try:
                await _generate_and_cache(
                    cluster_hash, representative,
                    confidence=confidence,
                    first_seen=first_seen,
                    last_seen=last_seen,
                    window_minutes=window_minutes,
                )
            finally:
                _PENDING.discard(cluster_hash)

        asyncio.create_task(_bg())

    return None
