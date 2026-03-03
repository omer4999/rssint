"""
Event clustering service.

Converts a flat list of raw Telegram messages into semantically grouped
``EventCluster`` objects using a four-stage pipeline.

Pipeline
--------
1. Pre-filter        : keep only messages from the last ``window_minutes`` that
                       pass the keyword relevance check, up to ``MAX_MESSAGES``.
2. Embed             : encode all filtered texts in a single batched call to the
                       SentenceTransformer model (no repeated inference).
3. Level-1 (Macro)   : greedy clustering with MACRO_SIM_THRESHOLD (0.80) AND a
                       30-minute time gate.  Groups broadly related messages
                       into macro-topics.
4. Level-2 (Hybrid)  : for each macro group, run the fingerprint layer first:
                         a. Extract structured event fields (actor, event_type,
                            target, location) from each message via rule-based
                            matching.
                         b. Generate a deterministic fingerprint string.
                         c. Messages sharing a valid fingerprint are grouped
                            directly into a sub-cluster — no embedding needed.
                         d. Messages without a valid fingerprint fall back to
                            embedding-based sub-clustering at SUB_SIM_THRESHOLD
                            (0.90).
5. Stage-3 (Centroid): after collecting all sub-clusters across every macro
                       group, merge pairs whose normalised centroid vectors
                       exceed CENTROID_MERGE_THRESHOLD (0.88) AND whose
                       first-seen timestamps are within MAX_TIME_DIFF_SECONDS.
                       Centroid is recomputed after every merge.
6. Enrich            : for each merged group compute source_count (unique
                       channels), confidence = min(1.0, channels / 5.0),
                       title, and deduplicate the display message list.
7. Sort              : all EventClusters ordered by source_count DESC,
                       last_seen DESC.

CPU-intensive work (embedding + similarity matrices) runs inside
``asyncio.get_event_loop().run_in_executor`` so the event loop is never blocked.
"""

from __future__ import annotations

import asyncio
import logging
import re
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import TYPE_CHECKING

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from models import Message
from services.relevance_service import is_relevant

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MACRO_SIM_THRESHOLD: float = 0.75
"""Level-1 cosine similarity threshold for broad macro-topic grouping."""

SUB_SIM_THRESHOLD: float = 0.85
"""Fallback embedding threshold for messages without a valid fingerprint."""

CENTROID_MERGE_THRESHOLD: float = 0.84
"""
Stage-3 threshold: two sub-clusters whose normalised centroids exceed this
cosine similarity (and are within MAX_TIME_DIFF_SECONDS) are merged into one.
"""

MAX_TIME_DIFF_SECONDS: int = 60 * 60
"""Time gate used at Level-1 macro clustering and Stage-3 centroid merge."""

DEDUP_THRESHOLD: float = 0.95
"""Legacy pairwise-matrix near-duplicate threshold (kept for reference)."""

SEMANTIC_DEDUP_THRESHOLD: float = 0.97
"""
Online semantic deduplication threshold used inside ``_semantic_dedup_messages``.
A candidate message is discarded when its cosine similarity to ANY already-kept
message meets or exceeds this value.
"""

# A fingerprint is considered valid only when at least this many fields are
# non-None.  If fewer fields match, we fall back to embedding clustering.
_MIN_FINGERPRINT_FIELDS: int = 2

MAX_MESSAGES: int = 200
DEFAULT_WINDOW_MINUTES: int = 30

_URL_RE: re.Pattern[str] = re.compile(r"https?://\S+", re.IGNORECASE)
_MARKDOWN_RE: re.Pattern[str] = re.compile(r"[*_`~#>|\\]")

# ---------------------------------------------------------------------------
# Embedding cache
# ---------------------------------------------------------------------------
# Telegram messages are immutable once stored, so (channel_name, message_id)
# is a stable key.  Caching avoids re-running the SentenceTransformer model on
# messages already seen in a previous request or polling cycle.
#
# Memory estimate: 10 000 entries × 384 floats × 4 bytes ≈ 15 MB — negligible.
# The lock protects the eviction logic from concurrent executor threads.

_EmbeddingKey = tuple[str, int]  # (channel_name, message_id)
_EMBEDDING_CACHE: dict[_EmbeddingKey, np.ndarray] = {}
_MAX_CACHE_SIZE: int = 10_000
_CACHE_LOCK: threading.Lock = threading.Lock()


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class ClusteredMessage:
    """A single message within an EventCluster, with its Telegram URL."""

    channel: str
    message_id: int
    text: str
    timestamp: datetime
    url: str


@dataclass
class EventCluster:
    """
    A group of semantically related messages drawn from one or more channels.

    Each instance is an independent intelligence event surfaced to the API.

    Attributes
    ----------
    event_id     : Stable UUID generated at cluster creation time.
    title        : Sanitised headline from the earliest message (≤ 120 chars).
    confidence   : Float in [0, 1]; min(1.0, unique_channel_count / 5.0).
    source_count : Number of UNIQUE channels that reported this event.
    channels     : Deduplicated, sorted list of contributing channel names.
    first_seen   : Timestamp of the oldest message in the cluster.
    last_seen    : Timestamp of the most recent message in the cluster.
    messages     : Deduplicated display messages (oldest-first). May be shorter
                   than the raw member list; source_count / channels unchanged.
    """

    event_id: uuid.UUID = field(default_factory=uuid.uuid4)
    title: str = ""
    summary: str = ""
    event_type: str = "unknown"
    actors: list[str] = field(default_factory=list)
    locations: list[str] = field(default_factory=list)
    impact_level: str = "low"
    confidence: float = 0.0
    source_count: int = 0
    channels: list[str] = field(default_factory=list)
    first_seen: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    last_seen: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    messages: list[ClusteredMessage] = field(default_factory=list)


@dataclass
class _RawGroup:
    """
    Intermediate structure carrying a cluster's members and their positions in
    the global embedding matrix.

    Used to pass data between pipeline stages without rebuilding embeddings.

    Attributes
    ----------
    members : ClusteredMessage list sorted oldest-first.
    indices : Positions of each member in the global ``embeddings`` array,
              in the same order as ``members``.
    """

    members: list[ClusteredMessage]
    indices: list[int]


@dataclass
class _CentroidGroup:
    """
    A sub-cluster augmented with a precomputed, L2-normalised centroid vector
    and time-bounds.  Used exclusively inside ``_centroid_merge_pass``.

    Attributes
    ----------
    members    : ClusteredMessage list sorted oldest-first.
    indices    : Positions in the global ``embeddings`` array (same order).
    centroid   : L2-normalised mean embedding vector for all members.
    first_seen : Timestamp of the oldest member.
    last_seen  : Timestamp of the most recent member.
    """

    members: list[ClusteredMessage]
    indices: list[int]
    centroid: np.ndarray
    first_seen: datetime
    last_seen: datetime


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _build_url(channel: str, message_id: int) -> str:
    """Return a ``t.me`` deep-link for the given channel and message."""
    return f"https://t.me/{channel.lstrip('@')}/{message_id}"


def _sanitise_title(text: str, max_len: int = 120) -> str:
    """Strip URLs and excessive whitespace, then truncate to *max_len*."""
    cleaned = _URL_RE.sub("", text)
    cleaned = " ".join(cleaned.split())
    return cleaned[:max_len].rstrip()


def _normalize_text(text: str) -> str:
    """
    Produce a canonical form of *text* used solely for exact-duplicate detection.

    Steps applied in order:
        1. Lowercase.
        2. Strip HTTP/HTTPS URLs.
        3. Remove Markdown symbols (*, _, `, ~, #, >, |, \\).
        4. Collapse runs of whitespace (including newlines) into a single space.
        5. Strip leading/trailing whitespace.

    Two messages whose normalised forms are identical are considered exact
    duplicates regardless of channel, capitalisation, or light formatting.
    """
    normalized = text.lower()
    normalized = _URL_RE.sub("", normalized)
    normalized = _MARKDOWN_RE.sub("", normalized)
    normalized = " ".join(normalized.split())
    return normalized


def _score_confidence(unique_channel_count: int) -> float:
    """``min(1.0, unique_channel_count / 5.0)``"""
    return min(1.0, unique_channel_count / 5.0)


def _pre_filter(
    messages: list[Message],
    window_minutes: int,
) -> list[Message]:
    """
    Return at most MAX_MESSAGES relevant messages within the time window,
    ordered by timestamp ascending so cluster seeds are earliest reports.

    When more than MAX_MESSAGES candidates exist the NEWEST ones are kept so
    that the most recent events are always represented in the output.  The
    final list is re-sorted oldest-first before returning so the greedy
    clustering algorithm can use the earliest message as the cluster seed.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
    candidates: list[Message] = []

    for msg in messages:
        if not msg.text:
            continue
        ts = msg.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts < cutoff:
            continue
        if not is_relevant(msg.text):
            continue
        candidates.append(msg)

    # Sort newest-first so the slice keeps the most recent MAX_MESSAGES.
    candidates.sort(key=lambda m: m.timestamp, reverse=True)
    candidates = candidates[:MAX_MESSAGES]
    # Re-sort oldest-first: the clustering algorithm seeds from the earliest
    # message, so ascending order is required for correct greedy grouping.
    candidates.sort(key=lambda m: m.timestamp)
    return candidates


def _compute_embeddings(
    messages: list[Message],
    model: "SentenceTransformer",
) -> np.ndarray:
    """
    Return an (N, D) embedding matrix for *messages*, serving cached vectors
    where available and batching only the uncached remainder.

    Cache key: ``(channel_name, message_id)`` — unique and stable for every
    Telegram message.  The returned matrix row order matches *messages* order.
    """
    keys: list[_EmbeddingKey] = [(m.channel_name, m.message_id) for m in messages]

    with _CACHE_LOCK:
        uncached_indices = [i for i, k in enumerate(keys) if k not in _EMBEDDING_CACHE]

    if uncached_indices:
        uncached_texts: list[str] = [messages[i].text for i in uncached_indices]  # type: ignore[misc]
        new_vecs: np.ndarray = model.encode(
            uncached_texts,
            batch_size=64,
            show_progress_bar=False,
            normalize_embeddings=True,
            convert_to_numpy=True,
        )
        with _CACHE_LOCK:
            # Evict oldest entries if the cache would overflow.
            overflow = (len(_EMBEDDING_CACHE) + len(uncached_indices)) - _MAX_CACHE_SIZE
            if overflow > 0:
                for old_key in list(_EMBEDDING_CACHE.keys())[:overflow]:
                    del _EMBEDDING_CACHE[old_key]
            for local_i, global_i in enumerate(uncached_indices):
                _EMBEDDING_CACHE[keys[global_i]] = new_vecs[local_i]

        logger.debug(
            "Embedding cache: %d new / %d from cache (total cached: %d).",
            len(uncached_indices),
            len(messages) - len(uncached_indices),
            len(_EMBEDDING_CACHE),
        )
    else:
        logger.debug(
            "Embedding cache: all %d messages served from cache.", len(messages)
        )

    # Reconstruct the full matrix in the original message order.
    with _CACHE_LOCK:
        result = np.stack([_EMBEDDING_CACHE[k] for k in keys])
    return result


def _deduplicate_display_messages(
    members: list[ClusteredMessage],
    member_embeddings: np.ndarray,
) -> list[ClusteredMessage]:
    """
    Remove near-duplicate messages from the display list only.

    The first occurrence (oldest) is kept.  source_count / channels are never
    recalculated here — this function is display-only.
    """
    k = len(members)
    if k <= 1:
        return members

    sim: np.ndarray = cosine_similarity(member_embeddings)
    keep: list[bool] = [True] * k

    for i in range(k):
        if not keep[i]:
            continue
        for j in range(i + 1, k):
            if keep[j] and sim[i, j] >= DEDUP_THRESHOLD:
                keep[j] = False

    deduped = [m for m, flag in zip(members, keep) if flag]
    removed = k - len(deduped)
    if removed:
        logger.debug("Deduplicated %d near-duplicate message(s) from sub-cluster.", removed)
    return deduped


def _exact_dedup_messages(
    members: list[ClusteredMessage],
) -> list[ClusteredMessage]:
    """
    Remove messages whose normalised text is identical to an earlier message.

    This is a second deduplication pass that runs after the embedding-based
    ``_deduplicate_display_messages``.  It catches cases where two messages
    have character-level identical content (modulo case, URLs, and Markdown
    formatting) that the cosine-similarity pass may still allow through when
    their embeddings happen to fall just below DEDUP_THRESHOLD.

    The first occurrence (oldest, since *members* is sorted ascending) is
    always kept as the representative.

    Does NOT affect source_count, confidence, or channels.
    """
    seen: set[str] = set()
    unique: list[ClusteredMessage] = []

    for msg in members:
        norm = _normalize_text(msg.text)
        if norm not in seen:
            seen.add(norm)
            unique.append(msg)

    removed = len(members) - len(unique)
    if removed:
        logger.debug(
            "Exact-dedup removed %d identical message(s) from cluster.", removed
        )
    return unique


def _semantic_dedup_messages(
    members: list[ClusteredMessage],
    member_embeddings: np.ndarray,
) -> list[ClusteredMessage]:
    """
    Remove semantically duplicate messages using an online accumulation strategy.

    For each message (oldest-first), compute its cosine similarity against every
    already-accepted unique message.  If the maximum similarity is below
    SEMANTIC_DEDUP_THRESHOLD (0.97), the message is genuinely novel and is kept;
    otherwise it is discarded as a semantic duplicate.

    Compared to the legacy pairwise-matrix approach in
    ``_deduplicate_display_messages``, this method:
    - Uses a stricter threshold (0.97 vs 0.95).
    - Checks against the full growing set of kept embeddings rather than a fixed
      seed, so a chain of gradually drifting paraphrases cannot slip through.

    Does NOT affect source_count, confidence, or channels.

    Parameters
    ----------
    members:
        Cluster messages sorted oldest-first.
    member_embeddings:
        (K, D) sub-array of normalised embeddings for *members*, in the same
        order.  Cosine similarity reduces to a dot product because vectors are
        L2-normalised.
    """
    unique_messages: list[ClusteredMessage] = []
    unique_embeddings: list[np.ndarray] = []

    for i, msg in enumerate(members):
        emb: np.ndarray = member_embeddings[i]

        if not unique_embeddings:
            unique_messages.append(msg)
            unique_embeddings.append(emb)
            continue

        # cosine_similarity expects 2-D arrays; result shape is (1, len(unique)).
        sims: np.ndarray = cosine_similarity([emb], unique_embeddings)[0]
        if float(sims.max()) < SEMANTIC_DEDUP_THRESHOLD:
            unique_messages.append(msg)
            unique_embeddings.append(emb)

    removed = len(members) - len(unique_messages)
    if removed:
        logger.debug(
            "Semantic dedup removed %d near-duplicate message(s) from cluster "
            "(threshold=%.2f).",
            removed,
            SEMANTIC_DEDUP_THRESHOLD,
        )
    return unique_messages


# ---------------------------------------------------------------------------
# Event fingerprint layer
# ---------------------------------------------------------------------------


def extract_event_fields(text: str) -> dict[str, str | None]:
    """
    Extract structured event fields from *text* using rule-based keyword matching.

    Supports both Hebrew and English keywords so the service handles
    multi-language Telegram channels without an NLP dependency.

    Returns
    -------
    dict with keys: ``actor``, ``event_type``, ``target``, ``location``.
    Each value is a normalised label string or ``None`` if not detected.
    """
    # ---- Actor ----
    actor: str | None = None
    if any(kw in text for kw in ["איראן", "Iran"]):
        actor = "IRAN"
    elif any(kw in text for kw in ["חזבאללה", "Hezbollah"]):
        actor = "HEZBOLLAH"
    elif any(kw in text for kw in ['ארה"ב', "USA", "United States"]):
        actor = "USA"
    elif any(kw in text for kw in ["סעודיה", "Saudi"]):
        actor = "SAUDI_ARABIA"

    # ---- Event type ----
    event_type: str | None = None
    if any(kw in text for kw in ["תקיפה", "strike", "attack", 'כטב"מ', "drone", "missile", "טיל"]):
        event_type = "STRIKE"
    elif any(kw in text for kw in ["התרסק", "crash", "מטוס"]):
        event_type = "CRASH"
    elif any(kw in text for kw in ["הצהיר", "statement", "אמר", "declared"]):
        event_type = "STATEMENT"
    elif any(kw in text for kw in ["ירי רקטות", "rocket fire"]):
        event_type = "ROCKET_FIRE"

    # ---- Target ----
    target: str | None = None
    if any(kw in text for kw in ["אראמכו", "Aramco"]):
        target = "ARAMCO"
    elif any(kw in text for kw in ["בסיס", "base"]):
        target = "MILITARY_BASE"
    elif any(kw in text for kw in ["נפט", "oil"]):
        target = "OIL_FACILITY"

    # ---- Location ----
    location: str | None = None
    if any(kw in text for kw in ["סעודיה", "Saudi"]):
        location = "SAUDI_ARABIA"
    elif any(kw in text for kw in ["לבנון", "Lebanon"]):
        location = "LEBANON"
    elif any(kw in text for kw in ["קפריסין", "Cyprus"]):
        location = "CYPRUS"

    return {
        "actor": actor,
        "event_type": event_type,
        "target": target,
        "location": location,
    }


def generate_fingerprint(fields: dict[str, str | None]) -> str | None:
    """
    Derive a deterministic identity string from extracted event fields.

    A fingerprint is only valid when at least ``_MIN_FINGERPRINT_FIELDS`` (2)
    of the four fields are non-None.  When fewer fields are populated, the
    message falls back to embedding-based sub-clustering.

    Parameters
    ----------
    fields:
        Output of ``extract_event_fields``.

    Returns
    -------
    str
        ``"<actor>|<event_type>|<target>|<location>"``  (None slots kept as-is
        in the string so distinct partial matches do not incorrectly collide).
    None
        When fewer than 2 fields matched — caller should use embedding fallback.
    """
    values = [
        fields["actor"],
        fields["event_type"],
        fields["target"],
        fields["location"],
    ]
    filled = sum(1 for v in values if v is not None)
    if filled < _MIN_FINGERPRINT_FIELDS:
        return None

    return f"{fields['actor']}|{fields['event_type']}|{fields['target']}|{fields['location']}"


# ---------------------------------------------------------------------------
# Level-1: Macro clustering
# ---------------------------------------------------------------------------


def _macro_greedy_pass(
    messages: list[Message],
    embeddings: np.ndarray,
) -> list[_RawGroup]:
    """
    Group *messages* into broad macro-topic clusters (Level 1).

    Gates per candidate:
        1. cosine_similarity(seed, candidate) >= MACRO_SIM_THRESHOLD (0.80)
        2. |seed.timestamp − candidate.timestamp| <= MAX_TIME_DIFF_SECONDS

    Returns
    -------
    list[_RawGroup]
        Members sorted oldest-first; ``indices`` map back to *embeddings* rows.
    """
    n = len(messages)
    if n == 0:
        return []

    sim_matrix: np.ndarray = cosine_similarity(embeddings)
    assigned: list[bool] = [False] * n
    groups: list[_RawGroup] = []

    for seed_idx in range(n):
        if assigned[seed_idx]:
            continue

        assigned[seed_idx] = True
        seed_msg = messages[seed_idx]
        seed_ts = seed_msg.timestamp
        if seed_ts.tzinfo is None:
            seed_ts = seed_ts.replace(tzinfo=timezone.utc)

        member_indices: list[int] = [seed_idx]
        raw_members: list[ClusteredMessage] = [
            ClusteredMessage(
                channel=seed_msg.channel_name,
                message_id=seed_msg.message_id,
                text=seed_msg.text or "",
                timestamp=seed_ts,
                url=_build_url(seed_msg.channel_name, seed_msg.message_id),
            )
        ]

        for cand_idx in range(seed_idx + 1, n):
            if assigned[cand_idx]:
                continue
            if sim_matrix[seed_idx, cand_idx] < MACRO_SIM_THRESHOLD:
                continue

            cand_msg = messages[cand_idx]
            cand_ts = cand_msg.timestamp
            if cand_ts.tzinfo is None:
                cand_ts = cand_ts.replace(tzinfo=timezone.utc)
            if abs((cand_ts - seed_ts).total_seconds()) > MAX_TIME_DIFF_SECONDS:
                continue

            assigned[cand_idx] = True
            member_indices.append(cand_idx)
            raw_members.append(
                ClusteredMessage(
                    channel=cand_msg.channel_name,
                    message_id=cand_msg.message_id,
                    text=cand_msg.text or "",
                    timestamp=cand_ts,
                    url=_build_url(cand_msg.channel_name, cand_msg.message_id),
                )
            )

        # Sort oldest-first, keeping member_indices aligned.
        paired = sorted(
            zip(raw_members, member_indices), key=lambda x: x[0].timestamp
        )
        raw_members = [p[0] for p in paired]
        member_indices = [p[1] for p in paired]

        groups.append(_RawGroup(members=raw_members, indices=member_indices))

    return groups


# ---------------------------------------------------------------------------
# Level-2: Hybrid fingerprint + embedding sub-clustering
# ---------------------------------------------------------------------------


def _embedding_sub_cluster(
    members: list[ClusteredMessage],
    global_indices: list[int],
    global_embeddings: np.ndarray,
) -> list[_RawGroup]:
    """
    Greedy embedding-based sub-clustering for messages that lack a valid
    fingerprint.

    Uses SUB_SIM_THRESHOLD (0.90), no time gate.  Re-uses already-computed
    embeddings via *global_indices* — no re-inference.
    """
    k = len(members)
    if k == 1:
        return [_RawGroup(members=members, indices=global_indices)]

    sub_embeddings: np.ndarray = global_embeddings[global_indices]
    sim_matrix: np.ndarray = cosine_similarity(sub_embeddings)

    assigned: list[bool] = [False] * k
    sub_groups: list[_RawGroup] = []

    for seed_local in range(k):
        if assigned[seed_local]:
            continue

        assigned[seed_local] = True
        cluster_local: list[int] = [seed_local]

        for cand_local in range(seed_local + 1, k):
            if assigned[cand_local]:
                continue
            if sim_matrix[seed_local, cand_local] >= SUB_SIM_THRESHOLD:
                assigned[cand_local] = True
                cluster_local.append(cand_local)

        sub_groups.append(
            _RawGroup(
                members=[members[i] for i in cluster_local],
                indices=[global_indices[i] for i in cluster_local],
            )
        )

    return sub_groups


def _fingerprint_sub_pass(
    macro_group: _RawGroup,
    global_embeddings: np.ndarray,
) -> list[_RawGroup]:
    """
    Hybrid Level-2 sub-clustering for a single macro group.

    Algorithm
    ---------
    1. For each message extract event fields → generate a fingerprint.
    2. Bucket messages that share a valid fingerprint — each bucket becomes
       one sub-cluster directly (no embedding distance needed).
    3. Messages without a valid fingerprint are collected and passed through
       ``_embedding_sub_cluster`` (greedy, SUB_SIM_THRESHOLD 0.90).

    Parameters
    ----------
    macro_group:
        Output of ``_macro_greedy_pass``; members already sorted oldest-first.
    global_embeddings:
        Full (N, D) embedding matrix; sub-arrays extracted via stored indices.

    Returns
    -------
    list[_RawGroup]
        One raw group per sub-cluster; each will be enriched into an
        ``EventCluster``.
    """
    members = macro_group.members
    k = len(members)

    if k == 1:
        return [macro_group]

    # ------------------------------------------------------------------
    # Step 1+2: Fingerprint routing
    # ------------------------------------------------------------------
    # fingerprint_buckets: fp → list of local indices into `members`
    fingerprint_buckets: dict[str, list[int]] = defaultdict(list)
    unfingerprinted_local: list[int] = []

    for local_idx, msg in enumerate(members):
        fields = extract_event_fields(msg.text)
        fp = generate_fingerprint(fields)
        if fp is not None:
            fingerprint_buckets[fp].append(local_idx)
            logger.debug(
                "Message %d assigned fingerprint '%s'.", msg.message_id, fp
            )
        else:
            unfingerprinted_local.append(local_idx)

    sub_groups: list[_RawGroup] = []

    # Each fingerprint bucket → one deterministic sub-cluster.
    for fp, local_indices in fingerprint_buckets.items():
        fp_members = [members[i] for i in local_indices]
        fp_global_indices = [macro_group.indices[i] for i in local_indices]
        # Ensure oldest-first within the bucket.
        paired = sorted(
            zip(fp_members, fp_global_indices), key=lambda x: x[0].timestamp
        )
        fp_members = [p[0] for p in paired]
        fp_global_indices = [p[1] for p in paired]
        sub_groups.append(_RawGroup(members=fp_members, indices=fp_global_indices))
        logger.debug(
            "Fingerprint bucket '%s': %d message(s) → 1 sub-cluster.", fp, len(fp_members)
        )

    # ------------------------------------------------------------------
    # Step 3: Embedding fallback for unfingerprinted messages
    # ------------------------------------------------------------------
    if unfingerprinted_local:
        unfp_members = [members[i] for i in unfingerprinted_local]
        unfp_global_indices = [macro_group.indices[i] for i in unfingerprinted_local]

        embedding_sub = _embedding_sub_cluster(
            unfp_members, unfp_global_indices, global_embeddings
        )
        sub_groups.extend(embedding_sub)
        logger.debug(
            "%d unfingerprinted message(s) → %d embedding sub-cluster(s).",
            len(unfingerprinted_local),
            len(embedding_sub),
        )

    return sub_groups


# ---------------------------------------------------------------------------
# Stage-3: Centroid merge
# ---------------------------------------------------------------------------


def _compute_centroid(
    indices: list[int],
    global_embeddings: np.ndarray,
) -> np.ndarray:
    """
    Compute and L2-normalise the mean embedding vector for the given *indices*.

    Parameters
    ----------
    indices:
        Row positions in *global_embeddings* to average.
    global_embeddings:
        Full (N, D) embedding matrix produced by ``_compute_embeddings``.

    Returns
    -------
    np.ndarray
        Shape (D,), L2-normalised.  If the mean vector is the zero vector
        (degenerate case) the unnormalised mean is returned unchanged.
    """
    sub: np.ndarray = global_embeddings[indices]
    centroid: np.ndarray = sub.mean(axis=0)
    norm: float = float(np.linalg.norm(centroid))
    if norm > 0.0:
        centroid = centroid / norm
    return centroid


def _centroid_merge_pass(
    raw_groups: list[_RawGroup],
    global_embeddings: np.ndarray,
) -> list[_RawGroup]:
    """
    Stage-3: greedily merge sub-clusters whose normalised centroids are very
    close in both semantic space and time.

    Algorithm
    ---------
    1. Compute a normalised centroid and extract time-bounds for every input
       sub-cluster, producing a list of ``_CentroidGroup`` objects.
    2. Iterate over each incoming group:
         - Compare its centroid against every already-merged group using
           ``sklearn.metrics.pairwise.cosine_similarity``.
         - If the best matching merged group satisfies BOTH:
               cosine_similarity(centroid_A, centroid_B) >= CENTROID_MERGE_THRESHOLD
               AND |first_seen_A − first_seen_B| <= MAX_TIME_DIFF_SECONDS
           → merge into that group: combine member / index lists, recompute the
             centroid from all pooled embeddings, update first/last seen.
         - Otherwise add as a new independent merged group.
    3. Convert merged ``_CentroidGroup`` objects back to ``_RawGroup`` so the
       existing ``_enrich_group`` function can consume them unchanged.

    Parameters
    ----------
    raw_groups:
        All sub-clusters collected across every macro group from Level-2.
    global_embeddings:
        Full (N, D) embedding matrix; used to extract centroid sub-arrays
        without any re-inference.

    Returns
    -------
    list[_RawGroup]
        Merged groups, each sorted oldest-first, ready for ``_enrich_group``.
    """
    if len(raw_groups) <= 1:
        return raw_groups

    # Build _CentroidGroup for every sub-cluster.
    centroid_groups: list[_CentroidGroup] = []
    for rg in raw_groups:
        timestamps = [m.timestamp for m in rg.members]
        centroid_groups.append(
            _CentroidGroup(
                members=rg.members,
                indices=rg.indices,
                centroid=_compute_centroid(rg.indices, global_embeddings),
                first_seen=min(timestamps),
                last_seen=max(timestamps),
            )
        )

    merged: list[_CentroidGroup] = []

    for incoming in centroid_groups:
        best_idx: int = -1
        best_sim: float = -1.0

        for j, existing in enumerate(merged):
            # sklearn cosine_similarity expects 2-D arrays.
            sim: float = float(
                cosine_similarity(
                    incoming.centroid.reshape(1, -1),
                    existing.centroid.reshape(1, -1),
                )[0, 0]
            )
            time_diff: float = abs(
                (incoming.first_seen - existing.first_seen).total_seconds()
            )

            if (
                sim >= CENTROID_MERGE_THRESHOLD
                and time_diff <= MAX_TIME_DIFF_SECONDS
                and sim > best_sim
            ):
                best_sim = sim
                best_idx = j

        if best_idx >= 0:
            target = merged[best_idx]

            # Combine members and indices, then re-sort oldest-first.
            combined_members = target.members + incoming.members
            combined_indices = target.indices + incoming.indices
            paired = sorted(
                zip(combined_members, combined_indices),
                key=lambda x: x[0].timestamp,
            )
            combined_members = [p[0] for p in paired]
            combined_indices = [p[1] for p in paired]

            merged[best_idx] = _CentroidGroup(
                members=combined_members,
                indices=combined_indices,
                centroid=_compute_centroid(combined_indices, global_embeddings),
                first_seen=min(target.first_seen, incoming.first_seen),
                last_seen=max(target.last_seen, incoming.last_seen),
            )
            logger.debug(
                "Centroid merge: absorbed %d message(s) into existing group "
                "(sim=%.4f, Δt=%.0fs).",
                len(incoming.members),
                best_sim,
                abs((incoming.first_seen - merged[best_idx].first_seen).total_seconds()),
            )
        else:
            merged.append(incoming)

    logger.debug(
        "Centroid merge pass: %d sub-clusters → %d merged clusters "
        "(threshold=%.2f, time≤%ds).",
        len(raw_groups),
        len(merged),
        CENTROID_MERGE_THRESHOLD,
        MAX_TIME_DIFF_SECONDS,
    )

    return [_RawGroup(members=cg.members, indices=cg.indices) for cg in merged]


# ---------------------------------------------------------------------------
# Enrichment: _RawGroup → EventCluster
# ---------------------------------------------------------------------------


def _enrich_group(
    group: _RawGroup,
    global_embeddings: np.ndarray,
) -> EventCluster:
    """
    Convert a ``_RawGroup`` into a fully enriched ``EventCluster``.

    1. source_count = number of UNIQUE channels (not total messages).
    2. confidence   = min(1.0, unique_channel_count / 5.0).
    3. title        = sanitised earliest message text (≤ 120 chars).
    4. display messages deduplicated without affecting source_count.
    """
    members = group.members  # already sorted oldest-first

    unique_channels: set[str] = {m.channel for m in members}
    source_count: int = len(unique_channels)
    confidence: float = _score_confidence(source_count)
    channels: list[str] = sorted(unique_channels)

    first_seen: datetime = members[0].timestamp
    last_seen: datetime = members[-1].timestamp
    title: str = _sanitise_title(members[0].text)

    member_embeddings: np.ndarray = global_embeddings[group.indices]
    # Pass 1: semantic dedup — online accumulation, cosine threshold 0.97.
    display_messages = _semantic_dedup_messages(members, member_embeddings)
    # Pass 2: exact dedup — remove character-identical texts (case/URL/markdown
    #          normalised) that may survive cosine similarity filtering.
    display_messages = _exact_dedup_messages(display_messages)

    return EventCluster(
        event_id=uuid.uuid4(),
        title=title,
        confidence=confidence,
        source_count=source_count,
        channels=channels,
        first_seen=first_seen,
        last_seen=last_seen,
        messages=display_messages,
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _run_clustering_sync(
    messages: list[Message],
    model: "SentenceTransformer",
    window_minutes: int,
) -> list[EventCluster]:
    """
    Full synchronous hybrid clustering pipeline.

    Intended to be called via ``run_in_executor`` — never call directly from
    an async context.
    """
    filtered = _pre_filter(messages, window_minutes)

    if not filtered:
        logger.debug("No relevant messages after pre-filtering; returning empty.")
        return []

    logger.debug("Computing embeddings for %d messages (cache active)…", len(filtered))

    embeddings: np.ndarray = _compute_embeddings(filtered, model)
    logger.debug("Embeddings shape: %s", embeddings.shape)

    # --- Level 1: macro clustering (similarity + time gate) ---
    macro_groups = _macro_greedy_pass(filtered, embeddings)
    logger.debug(
        "Level-1 macro clustering: %d messages → %d macro groups "
        "(sim≥%.2f, time≤%ds)",
        len(filtered),
        len(macro_groups),
        MACRO_SIM_THRESHOLD,
        MAX_TIME_DIFF_SECONDS,
    )

    # --- Level 2: hybrid fingerprint + embedding sub-clustering ---
    # Collect ALL sub-groups from every macro group into a single flat list
    # before running centroid merge so cross-macro merges are possible.
    all_raw_sub_groups: list[_RawGroup] = []
    for macro_group in macro_groups:
        sub_groups = _fingerprint_sub_pass(macro_group, embeddings)
        all_raw_sub_groups.extend(sub_groups)

    total_sub_before_merge = len(all_raw_sub_groups)

    # --- Stage 3: centroid merge across all sub-clusters ---
    merged_groups = _centroid_merge_pass(all_raw_sub_groups, embeddings)
    total_after_merge = len(merged_groups)

    logger.info(
        "Centroid merge: %d sub-clusters → %d merged clusters "
        "(centroid_sim≥%.2f, time≤%ds).",
        total_sub_before_merge,
        total_after_merge,
        CENTROID_MERGE_THRESHOLD,
        MAX_TIME_DIFF_SECONDS,
    )

    # --- Enrich merged groups into final EventCluster objects ---
    all_clusters: list[EventCluster] = [
        _enrich_group(group, embeddings) for group in merged_groups
    ]

    logger.info(
        "Pipeline complete: %d messages → %d macro groups → "
        "%d sub-clusters → %d final events "
        "(macro_sim≥%.2f, sub_sim≥%.2f, centroid_merge≥%.2f, "
        "time≤%ds, fp_min_fields=%d, dedup≥%.2f)",
        len(filtered),
        len(macro_groups),
        total_sub_before_merge,
        total_after_merge,
        MACRO_SIM_THRESHOLD,
        SUB_SIM_THRESHOLD,
        CENTROID_MERGE_THRESHOLD,
        MAX_TIME_DIFF_SECONDS,
        _MIN_FINGERPRINT_FIELDS,
        DEDUP_THRESHOLD,
    )
    return all_clusters


# ---------------------------------------------------------------------------
# Public async API
# ---------------------------------------------------------------------------


async def _add_summaries(
    clusters: list[EventCluster],
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
) -> list[EventCluster]:
    """
    Populate structured LLM analysis fields on each cluster.

    Clusters whose analysis is still being generated (not yet cached)
    are *excluded* from the returned list so the frontend never shows raw
    untranslated content.
    """
    from services import llm_service  # lazy import avoids circular deps

    async def _enrich_one(cluster: EventCluster) -> EventCluster | None:
        msg_texts = [m.text for m in cluster.messages]
        analysis = await llm_service.get_or_generate_analysis(
            msg_texts,
            confidence=cluster.confidence,
            first_seen=cluster.first_seen,
            last_seen=cluster.last_seen,
            window_minutes=window_minutes,
        )
        if analysis is None:
            return None
        cluster.title = analysis.title
        cluster.summary = analysis.summary
        cluster.event_type = analysis.event_type
        cluster.actors = analysis.actors
        cluster.locations = analysis.locations
        cluster.impact_level = analysis.impact_level
        return cluster

    results = await asyncio.gather(*[_enrich_one(c) for c in clusters])
    return [c for c in results if c is not None]


async def cluster_messages(
    messages: list[Message],
    model: "SentenceTransformer",
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
) -> list[EventCluster]:
    """
    Async entry point for the hybrid clustering pipeline.

    Offloads all CPU-bound work (embedding inference, similarity matrices,
    fingerprint extraction) to the default thread-pool executor so the
    event loop stays responsive.  After clustering completes, LLM-generated
    (or rule-based) summaries are added to every cluster via ``llm_service``.

    Parameters
    ----------
    messages:
        Raw ORM ``Message`` objects from the database.  May include irrelevant
        or out-of-window messages; the service filters them internally.
    model:
        SentenceTransformer instance loaded once at application startup
        (``app.state.embedding_model``).
    window_minutes:
        How far back (in minutes) to consider messages.

    Returns
    -------
    list[EventCluster]
        All sub-event clusters sorted by ``last_seen`` DESC — most-recent
        events first.  Each cluster carries a ``summary`` field.
    """
    loop = asyncio.get_event_loop()
    sync_fn = partial(_run_clustering_sync, messages, model, window_minutes)
    clusters: list[EventCluster] = await loop.run_in_executor(None, sync_fn)

    clusters.sort(key=lambda c: c.first_seen, reverse=True)

    # Generate / retrieve structured analysis for all clusters concurrently.
    # Clusters still awaiting GPT output are excluded so the frontend
    # never shows raw untranslated titles or content.
    clusters = await _add_summaries(clusters, window_minutes=window_minutes)

    return clusters
