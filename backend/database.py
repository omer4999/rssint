"""
Async SQLAlchemy engine and session factory.

Usage:
    async with get_session() as session:
        result = await session.execute(...)
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from config import Settings


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


def build_engine(settings: Settings):
    """
    Create the async SQLAlchemy engine from application settings.

    Pool parameters are tuned for a small production deployment.
    """
    return create_async_engine(
        settings.database_url,
        echo=settings.app_env == "development",
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )


def build_session_factory(settings: Settings) -> async_sessionmaker[AsyncSession]:
    """Return a session factory bound to the engine derived from *settings*."""
    engine = build_engine(settings)
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


@asynccontextmanager
async def get_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager that yields a database session.

    Commits on success and rolls back on any exception.
    """
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
