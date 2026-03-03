"""
RSSINT – Real-Time Structured Intelligence Platform
Embedding-based multi-channel event clustering + hourly briefs.

Startup sequence:
1. Validate settings from .env.
2. Create / verify all ORM tables (idempotent).
3. Initialize OpenAI embedding service.
4. Start the Telegram client.
5. Launch the background ingestion loop as an asyncio task.
6. Register API routers.

Shutdown sequence:
1. Cancel the ingestion task.
2. Disconnect the Telegram client.
3. Dispose the database engine.
"""
import os
import asyncio
import logging
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI

from config import Settings, get_settings
from database import Base, build_engine, build_session_factory
from routes import analysis, brief, developments, events, messages
from schemas import HealthResponse
from services.telegram_service import TelegramService
from telegram_ingest import run_ingestion_loop


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def configure_logging(log_level: str) -> None:
    """Set up structured logging for the entire application."""
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Build, configure, and return the FastAPI application instance."""

    settings: Settings = get_settings()
    configure_logging(settings.log_level)

    logger = logging.getLogger(__name__)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        """Manage startup and graceful shutdown of all long-lived resources."""

        # ----------------------------------------------------------------
        # Startup
        # ----------------------------------------------------------------
        logger.info("RSSINT backend starting up (env=%s).", settings.app_env)

        # --- Database ---
        engine = build_engine(settings)
        session_factory = build_session_factory(settings)

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database schema verified / created.")

        # --- Embedding service (OpenAI API) ---
        from services import embedding_service as _emb_svc  # noqa: PLC0415
        _emb_svc.init(settings.openai_api_key or "")
        logger.info("Embedding service initialized.")

        # --- LLM service (uses embedding service for dedup) ---
        from services import llm_service as _llm_svc  # noqa: PLC0415
        _llm_svc.init(session_factory, settings.openai_api_key or "")

        # --- Telegram ---
        telegram_service = TelegramService(
            settings=settings,
            session_factory=session_factory,
        )
        if os.getenv("ENABLE_TELEGRAM_INGEST", "false") == "true":
            await telegram_service.connect()

        ingestion_task = asyncio.create_task(
            run_ingestion_loop(telegram_service, settings),
            name="telegram_ingestion_loop",
        )
        logger.info("Telegram ingestion loop launched.")

        from analysis_job import run_analysis_loop  # noqa: PLC0415
        from development_graph_job import run_development_graph_loop  # noqa: PLC0415
        from developments_populate_job import run_developments_populate_loop  # noqa: PLC0415

        dev_graph_task = asyncio.create_task(
            run_development_graph_loop(
                session_factory,
                settings.openai_api_key or "",
            ),
            name="development_graph_loop",
        )
        logger.info("Development graph job launched.")

        dev_populate_task = asyncio.create_task(
            run_developments_populate_loop(session_factory),
            name="developments_populate_loop",
        )
        logger.info("Developments populate job launched (72h window).")

        analysis_task = asyncio.create_task(
            run_analysis_loop(session_factory, settings.openai_api_key or ""),
            name="analysis_loop",
        )
        logger.info("Analysis job launched (6h interval).")

        # Expose shared resources on app.state; route dependencies and
        # services retrieve them from here — no module-level globals.
        app.state.settings = settings
        app.state.session_factory = session_factory
        app.state.telegram_service = telegram_service

        yield

        # ----------------------------------------------------------------
        # Shutdown
        # ----------------------------------------------------------------
        logger.info("RSSINT backend shutting down…")

        ingestion_task.cancel()
        dev_graph_task.cancel()
        dev_populate_task.cancel()
        try:
            await ingestion_task
        except asyncio.CancelledError:
            logger.info("Ingestion loop stopped cleanly.")
        try:
            await dev_graph_task
        except asyncio.CancelledError:
            logger.info("Development graph job stopped cleanly.")
        try:
            await dev_populate_task
        except asyncio.CancelledError:
            logger.info("Developments populate job stopped cleanly.")
        analysis_task.cancel()
        try:
            await analysis_task
        except asyncio.CancelledError:
            logger.info("Analysis job stopped cleanly.")

        await telegram_service.disconnect()
        await engine.dispose()
        logger.info("Shutdown complete.")

    app = FastAPI(
        title="RSSINT",
        description=(
            "Geopolitical intelligence ingestion platform. "
            "Day 4: Embedding-based clustering + hourly intelligence briefs."
        ),
        version="0.3.0",
        lifespan=lifespan,
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -----------------------------------------------------------------------
    # Routers
    # -----------------------------------------------------------------------

    app.include_router(messages.router)
    app.include_router(events.router)
    app.include_router(brief.router)
    app.include_router(developments.router)
    app.include_router(analysis.router)

    # -----------------------------------------------------------------------
    # System routes
    # -----------------------------------------------------------------------

    @app.get(
        "/health",
        response_model=HealthResponse,
        summary="Health check",
        tags=["System"],
    )
    async def health() -> HealthResponse:
        """Return a simple liveness probe response."""
        return HealthResponse(status="ok")

    return app


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_config=None,
    )
