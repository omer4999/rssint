"""
SQLAlchemy ORM models for the RSSINT platform.
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from database import Base


class Message(Base):
    """
    Raw Telegram message as ingested from a monitored channel.

    Indexes:
    - uq_channel_message  : unique on (channel_name, message_id) for deduplication
    - ix_messages_timestamp_desc : descending timestamp for time-sorted queries
    - ix_messages_channel_name   : fast per-channel filtering
    - ix_messages_created_at     : fast ordering by ingestion time
    - ix_messages_text_fts       : GIN index on tsvector for full-text search
    """

    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("channel_name", "message_id", name="uq_channel_message"),
        # Descending timestamp index — most queries order by latest first.
        Index("ix_messages_timestamp_desc", "timestamp", postgresql_ops={"timestamp": "DESC"}),
        # Ascending created_at index for ingestion-time ordering.
        Index("ix_messages_created_at", "created_at"),
        # GIN full-text search index on the pre-computed tsvector column.
        Index("ix_messages_text_fts", "text_search_vector", postgresql_using="gin"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="Surrogate primary key",
    )
    channel_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,  # ix_messages_channel_name via shorthand
        comment="Telegram channel username or ID",
    )
    message_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Telegram-assigned message ID within the channel",
    )
    text: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Raw message text; NULL for media-only messages",
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Message date as reported by Telegram",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="Row insertion timestamp (UTC)",
    )
    processed: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default="false",
        nullable=False,
        comment="Flag set to true once downstream processing has consumed this row",
    )
    text_search_vector: Mapped[str | None] = mapped_column(
        TSVECTOR,
        nullable=True,
        comment=(
            "Pre-computed tsvector for full-text search. "
            "Populated via a Postgres trigger or manual UPDATE."
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<Message id={self.id} channel={self.channel_name!r} "
            f"message_id={self.message_id}>"
        )


class ClusterSummary(Base):
    """
    Persists LLM-generated summaries keyed by a content hash of the cluster's
    representative messages.

    Because the hash is derived from message *text* (not ephemeral cluster
    UUIDs), the same cluster content maps to the same row across server
    restarts, time-window changes, and re-clusterings — ensuring the OpenAI
    API is called at most once per unique event.
    """

    __tablename__ = "cluster_summaries"

    content_hash: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        comment="SHA-256 hex digest of the cluster's representative message texts",
    )
    summary: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="LLM-generated or rule-based neutral intelligence brief",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="Row insertion timestamp (UTC)",
    )

    def __repr__(self) -> str:
        return f"<ClusterSummary hash={self.content_hash[:12]}…>"


class StructuredEvent(Base):
    """
    Persisted structured intelligence event produced by the LLM.

    Keyed by ``cluster_hash`` — a deterministic hash of the cluster's
    representative message texts.  Ensures each unique cluster is analysed
    and stored at most once.
    """

    __tablename__ = "events"
    __table_args__ = (
        Index("ix_events_cluster_hash", "cluster_hash", unique=True),
        Index("ix_events_event_type", "event_type"),
        Index("ix_events_actors_gin", "actors", postgresql_using="gin"),
        Index("ix_events_locations_gin", "locations", postgresql_using="gin"),
    )

    EMBEDDING_DIM = 384

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    cluster_hash: Mapped[str] = mapped_column(
        Text, nullable=False, unique=True,
        comment="Deterministic hash of the cluster's representative texts",
    )
    title: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="",
        comment="LLM-generated headline",
    )
    summary: Mapped[str] = mapped_column(
        Text, nullable=False,
        comment="LLM-generated intelligence brief",
    )
    event_type: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default="unknown",
        comment="Classified event type",
    )
    impact_level: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="low",
        comment="Assessed impact level",
    )
    actors: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default="[]",
        comment="Key actors involved",
    )
    locations: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default="[]",
        comment="Affected locations",
    )
    confidence: Mapped[float] = mapped_column(
        Float, nullable=True,
        comment="Cluster confidence at generation time",
    )
    first_seen: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    last_seen: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    window_minutes: Mapped[int | None] = mapped_column(
        Integer, nullable=True,
    )
    embedding = mapped_column(
        JSONB, nullable=True,
        comment="384-d sentence embedding (stored as JSON array) for dedup",
    )
    development_processed: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default="false",
        nullable=False,
        comment="True once development graph relations have been computed",
    )
    development_source_count: Mapped[int] = mapped_column(
        Integer,
        default=1,
        server_default="1",
        nullable=False,
        comment="Number of merged events for development graph (1 = standalone)",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
        onupdate=func.now(), nullable=False,
    )

    def __repr__(self) -> str:
        return f"<StructuredEvent id={self.id} type={self.event_type}>"


class HourlyBrief(Base):
    """
    Pre-computed hourly intelligence summary.

    At most one row per UTC hour.  Generated once by the LLM and served to
    all users for the duration of that hour window.
    """

    __tablename__ = "hourly_briefs"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    window_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        comment="UTC hour start (floored)",
    )
    window_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="UTC hour end (window_start + 1 h)",
    )
    summary: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="LLM-generated situation summary for this hour",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="Row insertion timestamp (UTC)",
    )

    def __repr__(self) -> str:
        return f"<HourlyBrief id={self.id} window_start={self.window_start}>"


class EventRelation(Base):
    """
    Directed relation between two events, classified by LLM.

    Used for the cross-theater development graph.  Relations are computed
    in a background job, not during request handling.
    """

    __tablename__ = "event_relations"
    __table_args__ = (
        UniqueConstraint(
            "source_event_id", "target_event_id",
            name="uq_event_relation_source_target",
        ),
        Index("idx_event_rel_source", "source_event_id"),
        Index("idx_event_rel_target", "target_event_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    source_event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("events.id", ondelete="CASCADE"),
        nullable=False,
        comment="Earlier event (by first_seen)",
    )
    target_event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("events.id", ondelete="CASCADE"),
        nullable=False,
        comment="Later event (by first_seen)",
    )
    relation_type: Mapped[str] = mapped_column(
        String(50), nullable=False,
        comment="response|retaliation|escalation|continuation|political_reaction|strategic_signal",
    )
    confidence: Mapped[float] = mapped_column(
        Float, nullable=False,
        comment="LLM-assigned confidence 0.0–1.0",
    )
    reasoning: Mapped[str | None] = mapped_column(
        Text, nullable=True,
        comment="Short LLM explanation",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<EventRelation {self.source_event_id}→{self.target_event_id} {self.relation_type}>"


class ConflictAnalysis(Base):
    """
    Pre-computed conflict analysis from the developments graph.

    Generated every 6 hours by a background job. All users receive the same
    cached analysis until the next run.
    """

    __tablename__ = "conflict_analyses"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    conflict_overview: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="",
        comment="News-style overview of the full graph",
    )
    latest_developments: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="",
        comment="Brief of events from the last 6 hours",
    )
    possible_outcomes: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="",
        comment="Speculative geopolitical and financial implications",
    )
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
        comment="When this analysis was generated",
    )

    def __repr__(self) -> str:
        return f"<ConflictAnalysis id={self.id} generated_at={self.generated_at}>"
