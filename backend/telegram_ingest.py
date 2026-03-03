"""
Telegram ingestion background worker.

Runs a continuous loop that:
1. Loads the list of monitored channels from config/channels.txt.
2. Calls TelegramService.ingest_channel() for each channel sequentially.
3. Logs a per-cycle summary.
4. Sleeps for the configured interval before the next cycle.

This module is intentionally kept thin; all business logic lives in
services/telegram_service.py.
"""

import asyncio
import logging

from config import Settings
from services.telegram_service import TelegramService

logger = logging.getLogger(__name__)


async def run_ingestion_loop(
    service: TelegramService,
    settings: Settings,
) -> None:
    """
    Entry point for the ingestion background task.

    Designed to be launched via ``asyncio.create_task()`` from the FastAPI
    application lifespan so that it shares the same event loop.

    Parameters
    ----------
    service:
        Fully initialised TelegramService instance (client already connected).
    settings:
        Application settings used to read the interval and channel list.
    """
    interval = settings.ingest_interval_seconds
    logger.info(
        "Ingestion loop starting. Cycle interval: %d second(s).", interval
    )

    while True:
        channels = settings.load_channels()

        if not channels:
            logger.warning(
                "No channels configured. "
                "Add channel names to config/channels.txt."
            )
        else:
            logger.info(
                "--- Ingestion cycle started for %d channel(s). ---",
                len(channels),
            )
            total_new = 0
            total_errors = 0

            for channel in channels:
                report = await service.ingest_channel(channel)
                total_new += report.new_messages
                total_errors += report.errors

            logger.info(
                "--- Ingestion cycle complete. "
                "Total new messages: %d | Channels with errors: %d ---",
                total_new,
                total_errors,
            )

        await asyncio.sleep(interval)
