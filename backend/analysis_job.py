"""
6-hour background job: generate conflict analysis from the developments graph.

Runs every 6 hours. Fetches developments data, calls GPT-4o-mini, and persists
to conflict_analyses. All users receive the same cached analysis.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from openai import AsyncOpenAI
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import ConflictAnalysis, EventRelation, StructuredEvent

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import async_sessionmaker

logger = logging.getLogger(__name__)

_INTERVAL_SECONDS: int = 6 * 60 * 60  # 6 hours
_WINDOW_HOURS: int = 72
_LATEST_HOURS: int = 6
_DISCONNECTED_REMOVE_HOURS: int = 3
_MIN_CONFIDENCE: float = 0.4
_VALID_IMPACT: frozenset[str] = frozenset({"medium", "high", "critical"})

_PROMPT = """You are an intelligence analyst. You will receive a directed graph of conflict-related events and their causal links.

Generate a JSON object with exactly three keys:

1. "conflict_overview": A news-style, neutral, unbiased review of the entire graph. Summarize the situation, key actors, and how events connect. Write in a factual, reportorial tone. Remain neutral and avoid taking sides. 2–4 paragraphs.

2. "latest_developments": A brief of events from the last 6 hours only. News-style, neutral, unbiased. Report facts without editorializing. 1–2 paragraphs.

3. "possible_outcomes": Speculative analysis of near-future implications. Consider both geopolitical and financial dimensions. Do NOT state predictions with certainty. Use hedging language throughout: "may", "could", "might", "potential", "uncertain", "if X then Y could", "it is possible that". Frame explicitly as informed guesswork and speculation—not forecasts or predictions. 2–3 paragraphs.

Return ONLY valid JSON. No markdown, no code fences."""


def _build_graph_text(
    events: list[dict],
    edges: list[dict],
) -> str:
    """Build a text representation of the graph for the LLM."""
    event_by_id = {str(e["id"]): e for e in events}
    lines: list[str] = []

    lines.append("## Events")
    for e in sorted(events, key=lambda x: x.get("first_seen") or ""):
        actors = ", ".join(e.get("actors") or []) or "—"
        first_seen = e.get("first_seen")
        if isinstance(first_seen, datetime):
            first_seen = first_seen.isoformat() if first_seen else "N/A"
        lines.append(
            f"- [{e['id']}] {e['title']} | impact: {e['impact_level']} | "
            f"confidence: {e.get('confidence', 0):.0%} | actors: {actors} | {first_seen or 'N/A'}"
        )

    lines.append("\n## Causal Links (source → target, relation type)")
    for edge in edges:
        src_id = str(edge["source_event_id"])
        tgt_id = str(edge["target_event_id"])
        src = event_by_id.get(src_id)
        tgt = event_by_id.get(tgt_id)
        src_title = src["title"] if src else src_id
        tgt_title = tgt["title"] if tgt else tgt_id
        lines.append(f"- {src_title} → {tgt_title} [{edge['relation_type']}]")

    return "\n".join(lines)


def _filter_latest(events: list[dict], hours: int = _LATEST_HOURS) -> list[dict]:
    """Filter events to those in the last N hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    result: list[dict] = []
    for e in events:
        fs = e.get("first_seen")
        if fs is None:
            continue
        if isinstance(fs, datetime):
            dt = fs
        else:
            try:
                s = str(fs).replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
        if dt >= cutoff:
            result.append(e)
    return result if result else events[-10:] if len(events) > 10 else events


async def _fetch_developments(session: AsyncSession) -> tuple[list[dict], list[dict]]:
    """Fetch events and edges in the same format as the developments endpoint."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_WINDOW_HOURS)

    result = await session.execute(
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
        return [], []

    result = await session.execute(
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

    event_list: list[dict] = []
    for ev in events:
        if ev.id not in connected_ids:
            fs = ev.first_seen or ev.created_at
            if fs is None or fs < disconnected_cutoff:
                continue
        event_list.append({
            "id": str(ev.id),
            "title": ev.title,
            "impact_level": ev.impact_level,
            "confidence": ev.confidence or 0.0,
            "first_seen": ev.first_seen or ev.created_at,
            "actors": ev.actors or [],
        })

    kept_ids = {ev["id"] for ev in event_list}
    filtered = [
        r for r in relations
        if str(r.source_event_id) in kept_ids and str(r.target_event_id) in kept_ids
    ]

    seen_pairs: dict[tuple[str, str], dict] = {}
    for r in sorted(filtered, key=lambda x: -(x.confidence or 0.0)):
        a, b = str(r.source_event_id), str(r.target_event_id)
        pair = (min(a, b), max(a, b))
        if pair in seen_pairs:
            continue
        seen_pairs[pair] = {
            "source_event_id": a,
            "target_event_id": b,
            "relation_type": r.relation_type,
        }
    edge_list = list(seen_pairs.values())

    return event_list, edge_list


async def run_analysis_cycle(
    session_factory: "async_sessionmaker[AsyncSession]",
    openai_api_key: str,
) -> None:
    """One cycle: fetch developments, call LLM, persist analysis."""
    if not openai_api_key:
        logger.warning("Analysis job skipped: OPENAI_API_KEY not set.")
        return

    async with session_factory() as session:
        events, edges = await _fetch_developments(session)

    if not events:
        logger.info("Analysis job: no events in developments graph, skipping.")
        return

    graph_text = _build_graph_text(events, edges)
    latest_events = _filter_latest(events)
    latest_edge_ids = {e["id"] for e in latest_events}
    latest_edges = [
        e for e in edges
        if e["source_event_id"] in latest_edge_ids and e["target_event_id"] in latest_edge_ids
    ]
    latest_text = _build_graph_text(latest_events, latest_edges)

    user_content = f"""Full graph (72h window):

{graph_text}

---

Latest 6 hours subset:

{latest_text}

---

Generate the analysis JSON."""

    try:
        client = AsyncOpenAI(api_key=openai_api_key)
        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0.4,
        )
        raw = resp.choices[0].message.content or "{}"
        if raw.strip().startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        parsed = json.loads(raw.strip())
        conflict_overview = parsed.get("conflict_overview", "")
        latest_developments = parsed.get("latest_developments", "")
        possible_outcomes = parsed.get("possible_outcomes", "")
    except json.JSONDecodeError as e:
        logger.exception("Analysis LLM returned invalid JSON: %s", e)
        conflict_overview = "Analysis failed: invalid response format."
        latest_developments = ""
        possible_outcomes = ""
    except Exception as e:
        logger.exception("Analysis LLM error: %s", e)
        conflict_overview = f"Analysis failed: {e!s}"
        latest_developments = ""
        possible_outcomes = ""

    async with session_factory() as session:
        session.add(
            ConflictAnalysis(
                conflict_overview=conflict_overview,
                latest_developments=latest_developments,
                possible_outcomes=possible_outcomes,
            )
        )
        await session.commit()

    logger.info(
        "Analysis job: generated analysis (%d events, %d edges).",
        len(events), len(edges),
    )


async def run_analysis_loop(
    session_factory: "async_sessionmaker[AsyncSession]",
    openai_api_key: str,
) -> None:
    """Run analysis every 6 hours."""
    logger.info(
        "Analysis job started (interval=%ds).",
        _INTERVAL_SECONDS,
    )
    while True:
        try:
            await run_analysis_cycle(session_factory, openai_api_key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Analysis job error: %s", exc)
        await asyncio.sleep(_INTERVAL_SECONDS)
