"""
OpenAI embeddings service — replaces local SentenceTransformer.

Uses text-embedding-3-small (1536 dimensions) via OpenAI API.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openai import OpenAI

logger = logging.getLogger(__name__)

_OPENAI_CLIENT: "OpenAI | None" = None


def init(api_key: str | None = None) -> None:
    """Initialize the embedding service with API key."""
    global _OPENAI_API_KEY, _OPENAI_CLIENT  # noqa: PLW0603
    _OPENAI_API_KEY = (api_key or os.getenv("OPENAI_API_KEY") or "").strip()
    _OPENAI_CLIENT = None
    if _OPENAI_API_KEY:
        from openai import OpenAI
        _OPENAI_CLIENT = OpenAI(api_key=_OPENAI_API_KEY)
    logger.info(
        "Embedding service init: %s",
        "ready" if _OPENAI_CLIENT else "disabled (no API key)",
    )


def embed_texts_sync(texts: list[str]) -> list[list[float]]:
    """
    Generate embeddings for texts (sync, blocking).

    Use via run_in_executor from async code. Returns L2-normalized vectors.
    """
    if not _OPENAI_CLIENT or not texts:
        return []

    # OpenAI allows up to 2048 inputs per request; batch in chunks of 100
    batch_size = 100
    all_embeddings: list[list[float]] = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        # Truncate empty or very long texts
        cleaned = [t[:8191] if t else " " for t in batch]

        response = _OPENAI_CLIENT.embeddings.create(
            model="text-embedding-3-small",
            input=cleaned,
        )
        for d in response.data:
            emb = d.embedding
            # OpenAI returns normalized vectors; ensure list for consistency
            all_embeddings.append(list(emb))

    return all_embeddings


async def embed_texts(texts: list[str]) -> list[list[float]]:
    """
    Async wrapper: generate embeddings for texts via OpenAI API.

    Returns L2-normalized vectors (1536-d for text-embedding-3-small).
    """
    if not texts:
        return []

    return await asyncio.get_event_loop().run_in_executor(None, embed_texts_sync, texts)
