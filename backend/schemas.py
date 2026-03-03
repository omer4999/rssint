"""
Pydantic schemas used for data validation, serialisation, and API responses.
"""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Message schemas
# ---------------------------------------------------------------------------


class MessageBase(BaseModel):
    """Fields shared between creation and read schemas."""

    channel_name: str = Field(..., description="Telegram channel username or ID")
    message_id: int = Field(..., description="Telegram-assigned message ID")
    text: str | None = Field(None, description="Raw message text")
    timestamp: datetime = Field(..., description="Message date from Telegram")


class MessageCreate(MessageBase):
    """Schema used internally when inserting a new message row."""


class MessageRead(MessageBase):
    """Schema returned by API endpoints; includes database-generated fields."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    created_at: datetime
    processed: bool


class MessageListResponse(BaseModel):
    """Paginated / bounded list of messages."""

    total: int = Field(..., description="Number of messages returned in this response")
    messages: list[MessageRead]


# ---------------------------------------------------------------------------
# Clustered event schemas  (Day 3)
# ---------------------------------------------------------------------------


class EventCard(BaseModel):
    """
    A single message within a cluster, surfaced as an intelligence source.

    This is the leaf-level object inside ``EventClusterResponse``.
    """

    channel: str = Field(..., description="Source Telegram channel")
    timestamp: datetime = Field(..., description="When the message was sent")
    text: str = Field(..., description="Raw message text")
    url: str = Field(..., description="Telegram deep-link to the original message")


class StructuredEvent(BaseModel):
    """LLM-produced structured analysis of a cluster."""

    summary: str = Field("", description="Neutral 2–3 sentence intelligence brief")
    event_type: Literal[
        "military_strike", "rocket_attack", "terror_attack", "explosion",
        "armed_clash", "political_statement", "diplomatic_move", "sanctions",
        "protest", "internal_unrest", "infrastructure_damage", "cyber_attack",
        "unknown",
    ] = Field("unknown", description="Classified event type")
    actors: list[str] = Field(default_factory=list, description="Key actors involved")
    locations: list[str] = Field(default_factory=list, description="Affected locations")
    impact_level: Literal["low", "medium", "high", "critical"] = Field(
        "low", description="Assessed impact level"
    )


class EventClusterResponse(BaseModel):
    """
    A group of semantically related messages treated as a single intelligence event.
    """

    event_id: uuid.UUID = Field(..., description="Stable UUID for this cluster")
    title: str = Field(..., description="Auto-generated headline")
    summary: str = Field("", description="Neutral intelligence brief")
    event_type: str = Field("unknown", description="Classified event type")
    actors: list[str] = Field(default_factory=list, description="Key actors involved")
    locations: list[str] = Field(default_factory=list, description="Affected locations")
    impact_level: str = Field("low", description="Assessed impact level")
    confidence: float = Field(
        ..., ge=0.0, le=1.0,
        description="Cross-channel corroboration score",
    )
    source_count: int = Field(..., description="Number of unique channels in this cluster")
    channels: list[str] = Field(..., description="Unique channels contributing to this event")
    first_seen: datetime = Field(..., description="Timestamp of the earliest message")
    last_seen: datetime = Field(..., description="Timestamp of the most recent message")
    messages: list[EventCard] = Field(..., description="All constituent messages")


class EventsResponse(BaseModel):
    """Response envelope for the /events/latest endpoint."""

    total: int = Field(..., description="Number of clusters returned")
    window_minutes: int = Field(..., description="Look-back window used for this response")
    events: list[EventClusterResponse]


# ---------------------------------------------------------------------------
# Hourly brief schema
# ---------------------------------------------------------------------------


class HourlyBriefResponse(BaseModel):
    """Response for the GET /brief/hourly endpoint."""

    model_config = ConfigDict(from_attributes=True)

    window_start: datetime = Field(..., description="UTC hour start")
    window_end: datetime = Field(..., description="UTC hour end")
    summary: str = Field(..., description="LLM-generated situation summary")


# ---------------------------------------------------------------------------
# Health check schema
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    """Response body for the /health endpoint."""

    status: str = Field("ok", description="Service health status")
    messages_24h: int | None = Field(None, description="Messages in last 24h (when ?db=1)")
    events_24h: int | None = Field(None, description="Events in last 24h (when ?db=1)")


class DiagnosticsResponse(BaseModel):
    """DB and ingestion diagnostics for deployment debugging."""

    messages_total: int = Field(0, description="Total messages in DB (all time)")
    events_total: int = Field(0, description="Total events in DB (all time)")
    telegram_ingest_enabled: bool = Field(False, description="ENABLE_TELEGRAM_INGEST")
    hint: str = Field("", description="Suggested fix when DB is empty")


# ---------------------------------------------------------------------------
# Ingestion report schema (used in logs / future API)
# ---------------------------------------------------------------------------


class IngestionReport(BaseModel):
    """Summary produced after one ingestion cycle."""

    channel: str
    new_messages: int
    errors: int = 0
