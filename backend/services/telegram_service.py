"""
Telegram service layer.

Responsible for:
- Maintaining a single shared Telethon client instance.
- Fetching messages from a given channel.
- Persisting new messages to the database, deduplicating by (channel_name, message_id).
"""

import logging
from datetime import timezone

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telethon import TelegramClient
from telethon.tl.types import Message as TelegramMessage

from config import Settings
from models import Message
from schemas import IngestionReport, MessageCreate

logger = logging.getLogger(__name__)


class TelegramService:
    """
    Encapsulates all interactions with the Telegram API and the messages table.

    Parameters
    ----------
    settings:
        Application settings, used to initialise the Telethon client.
    session_factory:
        SQLAlchemy async session factory injected from the application lifespan.
    """

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._client: TelegramClient = TelegramClient(
            "bosint_session",
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )

    # ------------------------------------------------------------------
    # Client lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Start the Telethon client and authenticate."""
        await self._client.start()
        logger.info("Telegram client connected successfully.")

    async def disconnect(self) -> None:
        """Gracefully disconnect the Telethon client."""
        await self._client.disconnect()
        logger.info("Telegram client disconnected.")

    # ------------------------------------------------------------------
    # Core ingestion logic
    # ------------------------------------------------------------------

    async def ingest_channel(self, channel: str) -> IngestionReport:
        """
        Fetch the latest messages from *channel* and persist new ones to the DB.

        Deduplication is handled at the database level using an ON CONFLICT DO
        NOTHING clause keyed on the (channel_name, message_id) unique index.

        Parameters
        ----------
        channel:
            Telegram channel username (e.g. ``"bbcnews"``) or numeric ID.

        Returns
        -------
        IngestionReport
            Summary of how many new rows were inserted and any errors.
        """
        new_count = 0
        error_count = 0

        try:
            messages: list[MessageCreate] = await self._fetch_messages(channel)

            if not messages:
                logger.debug("No messages fetched from channel '%s'.", channel)
                return IngestionReport(channel=channel, new_messages=0)

            new_count = await self._persist_messages(messages)
            logger.info(
                "Channel '%s': fetched %d message(s), inserted %d new.",
                channel,
                len(messages),
                new_count,
            )

        except Exception as exc:
            error_count += 1
            logger.error(
                "Error ingesting channel '%s': %s",
                channel,
                exc,
                exc_info=True,
            )

        return IngestionReport(
            channel=channel,
            new_messages=new_count,
            errors=error_count,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _fetch_messages(self, channel: str) -> list[MessageCreate]:
        """
        Retrieve the most recent messages from a Telegram channel.

        Fetches up to 100 messages per cycle.  Only messages that carry text
        are converted to ``MessageCreate`` instances; media-only posts with no
        caption are still stored with ``text=None``.
        """
        raw_messages: list[TelegramMessage] = await self._client.get_messages(
            channel,
            limit=100,
        )

        result: list[MessageCreate] = []
        for msg in raw_messages:
            if not isinstance(msg, TelegramMessage):
                continue

            timestamp = msg.date
            if timestamp is not None and timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            result.append(
                MessageCreate(
                    channel_name=channel,
                    message_id=msg.id,
                    text=msg.text or None,
                    timestamp=timestamp,
                )
            )

        return result

    async def _persist_messages(self, messages: list[MessageCreate]) -> int:
        """
        Bulk-insert *messages*, skipping rows that already exist.

        Returns the number of rows actually inserted.
        """
        if not messages:
            return 0

        rows = [m.model_dump() for m in messages]

        async with self._session_factory() as session:
            stmt = (
                pg_insert(Message)
                .values(rows)
                .on_conflict_do_nothing(
                    constraint="uq_channel_message",
                )
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def get_latest_message_id(
        self,
        channel: str,
        session: AsyncSession,
    ) -> int | None:
        """
        Return the highest Telegram message_id already stored for *channel*.

        Useful for future optimisation to only fetch messages newer than the
        last known ID.
        """
        stmt = (
            select(Message.message_id)
            .where(Message.channel_name == channel)
            .order_by(Message.message_id.desc())
            .limit(1)
        )
        row = (await session.execute(stmt)).scalar_one_or_none()
        return row
