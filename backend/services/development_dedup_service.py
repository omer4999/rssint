"""
Development deduplication service.

Determines whether two events represent the SAME development using the same
OpenAI model as event summarization.  Used only by the 30-minute background
job; never called from request handlers.
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

_MODEL = "gpt-4o-mini"
_MIN_CONFIDENCE: float = 0.7

_SYSTEM_PROMPT = """You are an intelligence analyst.

Determine whether Event B represents the SAME development as Event A.

Two events are the SAME development if:
- They describe the same operational action
- They report the same incident instance in time
- They are wording variations of the same event

Return strict JSON:
{
  "is_duplicate": true/false,
  "confidence": 0.0-1.0,
  "reasoning": "short explanation"
}

Rules:
- Be conservative.
- Only return true if clearly the same development.
- If unsure, return false.
- Only return confidence >= 0.7 if confident.
- Do not add commentary.
- Return only JSON."""


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


def _parse_json(raw: str) -> tuple[bool, float] | None:
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```\s*$", "", raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.debug("Dedup JSON parse failed: %s", raw[:200])
        return None

    if not isinstance(data, dict):
        return None

    is_dup = data.get("is_duplicate")
    conf = data.get("confidence")

    if not isinstance(is_dup, bool):
        return None

    try:
        conf_f = float(conf)
    except (TypeError, ValueError):
        return None

    if not (0.0 <= conf_f <= 1.0):
        return None

    return (is_dup, conf_f)


async def classify_duplicate(
    new_event: object,
    existing_event: object,
    openai_client: "AsyncOpenAI",
) -> bool:
    """
    Return True only if is_duplicate == true AND confidence >= 0.7.
    If malformed or OpenAI fails, return False (treat as not duplicate).
    """
    user_content = (
        "Event A:\n"
        f"{_format_event(existing_event)}\n\n"
        "Event B:\n"
        f"{_format_event(new_event)}\n"
    )

    try:
        response = await openai_client.chat.completions.create(
            model=_MODEL,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            max_tokens=150,
            temperature=0,
        )
        raw = response.choices[0].message.content or ""
    except Exception as exc:
        logger.warning("Dedup OpenAI call failed: %s", exc)
        return False

    parsed = _parse_json(raw)
    if parsed is None:
        return False

    is_dup, conf = parsed
    if not is_dup or conf < _MIN_CONFIDENCE:
        return False

    return True
