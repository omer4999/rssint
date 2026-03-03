"""
One-shot: delete all rows from the events table so they regenerate
with proper deduplication and embeddings.

Run once:
    python _purge_events.py
"""

import asyncio
import sys

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from config import get_settings


async def main() -> None:
    settings = get_settings()
    engine = create_async_engine(settings.database_url)

    async with engine.begin() as conn:
        result = await conn.execute(text("DELETE FROM events"))
        print(f"Deleted {result.rowcount} rows from events table.")

    await engine.dispose()
    print("Purge complete — events will regenerate on next poll.")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
