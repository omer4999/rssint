"""
Message endpoints.

GET /messages/latest          – most recent N messages across all channels
GET /messages/channel/{name}  – messages from a specific channel
GET /messages/search          – keyword substring search
GET /messages                 – paginated list with total count
"""

import logging
import math
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from repositories import message_repository as repo
from routes.deps import get_db
from schemas import MessageListResponse, MessageRead

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/messages", tags=["Messages"])

# ---------------------------------------------------------------------------
# Type alias for the injected DB session
# ---------------------------------------------------------------------------

DbSession = Annotated[AsyncSession, Depends(get_db)]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/latest",
    response_model=MessageListResponse,
    summary="Latest messages",
    description="Return the most recent messages across all monitored channels.",
)
async def latest_messages(
    db: DbSession,
    limit: int = Query(default=50, ge=1, le=200, description="Max messages to return"),
) -> MessageListResponse:
    """Fetch the *limit* most recent messages ordered by timestamp descending."""
    rows = await repo.get_latest_messages(db, limit=limit)
    logger.info("GET /messages/latest limit=%d → %d results", limit, len(rows))
    return MessageListResponse(
        total=len(rows),
        messages=[MessageRead.model_validate(r) for r in rows],
    )


@router.get(
    "/channel/{channel}",
    response_model=MessageListResponse,
    summary="Messages by channel",
    description="Return the most recent messages from a specific Telegram channel.",
)
async def messages_by_channel(
    channel: str,
    db: DbSession,
    limit: int = Query(default=50, ge=1, le=200, description="Max messages to return"),
) -> MessageListResponse:
    """Fetch the latest messages from *channel*, ordered by timestamp descending."""
    rows = await repo.get_messages_by_channel(db, channel=channel, limit=limit)
    logger.info(
        "GET /messages/channel/%s limit=%d → %d results", channel, limit, len(rows)
    )
    return MessageListResponse(
        total=len(rows),
        messages=[MessageRead.model_validate(r) for r in rows],
    )


@router.get(
    "/search",
    response_model=MessageListResponse,
    summary="Search messages",
    description="Case-insensitive substring search across message text.",
)
async def search_messages(
    q: str = Query(..., min_length=1, description="Keyword to search for"),
    limit: int = Query(default=50, ge=1, le=200, description="Max messages to return"),
    db: AsyncSession = Depends(get_db),
) -> MessageListResponse:
    """Full-text keyword search using PostgreSQL ILIKE."""
    rows = await repo.search_messages(db, keyword=q, limit=limit)
    logger.info(
        "GET /messages/search q=%r limit=%d → %d results", q, limit, len(rows)
    )
    return MessageListResponse(
        total=len(rows),
        messages=[MessageRead.model_validate(r) for r in rows],
    )


@router.get(
    "",
    response_model=MessageListResponse,
    summary="Paginated messages",
    description="Return a page of messages with total count for pagination.",
)
async def paginated_messages(
    db: DbSession,
    page: int = Query(default=1, ge=1, description="1-based page number"),
    limit: int = Query(default=50, ge=1, le=200, description="Page size"),
) -> MessageListResponse:
    """
    Return ``limit`` messages starting at the correct offset for ``page``.

    The ``total`` field in the response contains the full table count, allowing
    clients to calculate the number of pages as ``ceil(total / limit)``.
    """
    offset = (page - 1) * limit
    rows, total = await repo.get_paginated_messages(db, limit=limit, offset=offset)
    total_pages = math.ceil(total / limit) if total else 1
    logger.info(
        "GET /messages page=%d limit=%d → %d/%d rows (%d pages)",
        page,
        limit,
        len(rows),
        total,
        total_pages,
    )
    return MessageListResponse(
        total=total,
        messages=[MessageRead.model_validate(r) for r in rows],
    )
