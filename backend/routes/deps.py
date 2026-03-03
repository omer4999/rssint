"""
FastAPI dependency providers shared across all routers.

Inject ``get_db`` into route functions to obtain a request-scoped
``AsyncSession`` that is automatically closed after the response.
"""

from collections.abc import AsyncGenerator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


async def get_db(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """
    Yield a database session scoped to the current HTTP request.

    The session factory is retrieved from ``app.state``, which is populated
    during application startup in ``main.py``.
    """
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    async with factory() as session:
        yield session
