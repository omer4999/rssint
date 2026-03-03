"""
One-shot migration: add embedding JSONB column to the events table.

Run once:
    python _migrate_pgvector.py
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
        await conn.execute(text("""
            ALTER TABLE events
            ADD COLUMN IF NOT EXISTS embedding JSONB
        """))
        print("embedding JSONB column added (or already exists).")

    await engine.dispose()
    print("Migration complete.")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
