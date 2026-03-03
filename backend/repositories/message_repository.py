"""
Message repository.

All database access for the ``messages`` table is centralised here.
Routes and services must not issue raw SQL or ORM queries directly —
they call these repository functions instead.

Every function accepts an ``AsyncSession`` injected by the caller, keeping
transaction control in the calling layer.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Message

logger = logging.getLogger(__name__)


async def get_latest_messages(
    session: AsyncSession,
    limit: int = 50,
) -> list[Message]:
    """
    Return the most recent *limit* messages across all channels, newest first.

    Parameters
    ----------
    session:
        Active async SQLAlchemy session.
    limit:
        Maximum number of rows to return (capped at 200 to protect the DB).
    """
    effective_limit = min(limit, 200)
    stmt = (
        select(Message)
        .order_by(Message.timestamp.desc())
        .limit(effective_limit)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    logger.debug("get_latest_messages(limit=%d) → %d rows", effective_limit, len(rows))
    return list(rows)


async def get_messages_by_channel(
    session: AsyncSession,
    channel: str,
    limit: int = 50,
) -> list[Message]:
    """
    Return the most recent messages from a specific channel.

    Parameters
    ----------
    session:
        Active async SQLAlchemy session.
    channel:
        Telegram channel username or ID to filter by.
    limit:
        Maximum number of rows to return (capped at 200).
    """
    effective_limit = min(limit, 200)
    stmt = (
        select(Message)
        .where(Message.channel_name == channel)
        .order_by(Message.timestamp.desc())
        .limit(effective_limit)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    logger.debug(
        "get_messages_by_channel(channel=%r, limit=%d) → %d rows",
        channel,
        effective_limit,
        len(rows),
    )
    return list(rows)


async def get_recent_messages(
    session: AsyncSession,
    minutes: int = 30,
) -> list[Message]:
    """
    Return all messages whose ``timestamp`` falls within the last *minutes* minutes.

    Results are ordered by timestamp descending (most recent first).

    Parameters
    ----------
    session:
        Active async SQLAlchemy session.
    minutes:
        Look-back window in minutes.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=minutes)
    stmt = (
        select(Message)
        .where(Message.timestamp >= cutoff)
        .order_by(Message.timestamp.desc())
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    logger.debug(
        "get_recent_messages(minutes=%d) → %d rows (cutoff=%s)",
        minutes,
        len(rows),
        cutoff.isoformat(),
    )
    return list(rows)


async def search_messages(
    session: AsyncSession,
    keyword: str,
    limit: int = 50,
) -> list[Message]:
    """
    Return messages whose ``text`` contains *keyword* (case-insensitive).

    Uses the PostgreSQL ``ILIKE`` operator for a simple substring match.
    For large datasets consider switching to the ``text_search_vector`` GIN
    index with ``to_tsquery``.

    Parameters
    ----------
    session:
        Active async SQLAlchemy session.
    keyword:
        Substring to search for.
    limit:
        Maximum number of rows to return (capped at 200).
    """
    effective_limit = min(limit, 200)
    pattern = f"%{keyword}%"
    stmt = (
        select(Message)
        .where(Message.text.ilike(pattern))
        .order_by(Message.timestamp.desc())
        .limit(effective_limit)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    logger.debug(
        "search_messages(keyword=%r, limit=%d) → %d rows",
        keyword,
        effective_limit,
        len(rows),
    )
    return list(rows)


async def get_paginated_messages(
    session: AsyncSession,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[Message], int]:
    """
    Return a page of messages and the total row count.

    Results are ordered by timestamp descending.

    Parameters
    ----------
    session:
        Active async SQLAlchemy session.
    limit:
        Page size (capped at 200).
    offset:
        Number of rows to skip (0-based).

    Returns
    -------
    tuple[list[Message], int]
        ``(rows, total_count)`` where *total_count* is the count of all
        messages in the table (used to build pagination metadata).
    """
    effective_limit = min(limit, 200)

    count_stmt = select(func.count()).select_from(Message)
    total: int = (await session.execute(count_stmt)).scalar_one()

    rows_stmt = (
        select(Message)
        .order_by(Message.timestamp.desc())
        .limit(effective_limit)
        .offset(offset)
    )
    rows = list((await session.execute(rows_stmt)).scalars().all())

    logger.debug(
        "get_paginated_messages(limit=%d, offset=%d) → %d/%d rows",
        effective_limit,
        offset,
        len(rows),
        total,
    )
    return rows, total
