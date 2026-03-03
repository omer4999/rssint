"""
Development relation classification service.

Classifies whether two events are related (response, retaliation, escalation,
etc.) using the same OpenAI model as event summarization.  Used only by the
30-minute background job; never called from request handlers.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

_MODEL = "gpt-4o-mini"
_MIN_CONFIDENCE: float = 0.6
_VALID_RELATION_TYPES = frozenset({
    "response", "retaliation", "escalation", "continuation",
    "political_reaction", "strategic_signal", "none",
})

_SYSTEM_PROMPT = """You are an intelligence analyst.
Determine whether the second event is related to the first event in terms of:
- response
- retaliation
- escalation
- continuation
- political_reaction
- strategic_signal
- none

ACTORS: Pay close attention to the actors field. Shared or related actors
(e.g., same state, same armed group, opposing parties in a conflict) strongly
suggest a causal or narrative connection. Consider:
- Direct overlap: same actor appears in both events
- Related actors: allied states, rival factions, state vs non-state actors
- Actor chains: Event A involves X, Event B involves X's response or X's adversary

Return strict JSON:
{
  "relation_type": "...",
  "confidence": 0.0-1.0,
  "reasoning": "short explanation (mention actors when relevant)"
}

Only return JSON.
Do not add commentary."""


@dataclass
class RelationResult:
    relation_type: str
    confidence: float
    reasoning: str


def _format_event(ev: object) -> str:
    title = getattr(ev, "title", "") or ""
    summary = getattr(ev, "summary", "") or ""
    actors = getattr(ev, "actors", None) or []
    locations = getattr(ev, "locations", None) or []
    first_seen = getattr(ev, "first_seen", None)
    time_str = str(first_seen) if first_seen else "—"
    return (
        f"Title: {title}\n"
        f"Summary: {summary}\n"
        f"Actors: {actors}\n"
        f"Locations: {locations}\n"
        f"Time: {time_str}"
    )


def _parse_json(raw: str) -> RelationResult | None:
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```\s*$", "", raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.debug("Relation JSON parse failed: %s", raw[:200])
        return None

    if not isinstance(data, dict):
        return None

    rt = str(data.get("relation_type", "")).strip().lower()
    conf = data.get("confidence")
    reasoning = str(data.get("reasoning", "")).strip()

    if rt not in _VALID_RELATION_TYPES:
        return None

    try:
        conf_f = float(conf)
    except (TypeError, ValueError):
        return None

    if not (0.0 <= conf_f <= 1.0):
        return None

    return RelationResult(relation_type=rt, confidence=conf_f, reasoning=reasoning)


async def classify_relation(
    new_event: object,
    candidate_event: object,
    openai_client: "AsyncOpenAI",
) -> RelationResult | None:
    """
    Classify whether candidate_event is related to new_event.

    Returns None if relation_type == "none" OR confidence < 0.6.
    Rejects malformed responses.
    """
    user_content = (
        "Event A:\n"
        f"{_format_event(new_event)}\n\n"
        "Event B:\n"
        f"{_format_event(candidate_event)}\n\n"
        "Rules:\n"
        "- Consider actors: shared or related actors (same conflict parties, state vs non-state) strengthen the case for connection.\n"
        "- Cross-theater relations ARE allowed (e.g. Actor A in region X responds to Actor B in region Y).\n"
        "- Do not require shared location.\n"
        "- Be conservative; if unrelated, return relation_type = \"none\".\n"
        "- Only return confidence >= 0.6 if reasonably certain.\n"
    )

    try:
        response = await openai_client.chat.completions.create(
            model=_MODEL,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            max_tokens=200,
            temperature=0,
        )
        raw = response.choices[0].message.content or ""
    except Exception as exc:
        logger.warning("Relation classification OpenAI call failed: %s", exc)
        return None

    result = _parse_json(raw)
    if result is None:
        return None

    if result.relation_type == "none":
        return None

    if result.confidence < _MIN_CONFIDENCE:
        return None

    return result
