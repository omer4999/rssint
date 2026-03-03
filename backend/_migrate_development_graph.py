"""
One-shot migration: add development_processed column and event_relations table.

Run once:
    python _migrate_development_graph.py
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
            ADD COLUMN IF NOT EXISTS development_processed BOOLEAN DEFAULT FALSE
        """))
        print("development_processed column added (or already exists).")

        await conn.execute(text("""
            ALTER TABLE events
            ADD COLUMN IF NOT EXISTS development_source_count INTEGER DEFAULT 1
        """))
        print("development_source_count column added (or already exists).")

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS event_relations (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_event_id UUID REFERENCES events(id) ON DELETE CASCADE,
                target_event_id UUID REFERENCES events(id) ON DELETE CASCADE,
                relation_type TEXT NOT NULL,
                confidence FLOAT NOT NULL,
                reasoning TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                CONSTRAINT uq_event_relation_source_target UNIQUE (source_event_id, target_event_id)
            )
        """))
        print("event_relations table created (or already exists).")

        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_event_rel_source
            ON event_relations(source_event_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_event_rel_target
            ON event_relations(target_event_id)
        """))
        print("Indexes created (or already exist).")

    await engine.dispose()
    print("Migration complete.")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
