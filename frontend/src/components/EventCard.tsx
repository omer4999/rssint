import { useState, type FC } from "react";
import type { Event, EventMessage, ImpactLevel } from "../types/event";
import ConfidenceBadge from "./ConfidenceBadge";
import { formatLocalTime } from "../utils/time";

interface EventCardProps {
  event: Event;
  /** When true the card plays a brief blue-glow entrance animation. */
  isNew?: boolean;
}

const MAX_MESSAGES_SHOWN = 5;
const MAX_TEXT_CHARS = 300;

/** Truncates text with ellipsis if it exceeds maxLen. */
function truncate(text: string, maxLen: number): string {
  if (text.length <= maxLen) return text;
  return text.slice(0, maxLen).trimEnd() + "…";
}

/**
 * Strips common Markdown syntax from a string so it renders as clean plain
 * text.  Does NOT use dangerouslySetInnerHTML or any markdown parser.
 */
function stripMarkdown(text: string): string {
  return text
    .replace(/!\[.*?\]\(.*?\)/g, "")        // images
    .replace(/\[(.+?)\]\(.*?\)/g, "$1")     // [label](url) → label
    .replace(/\*\*(.+?)\*\*/g, "$1")        // **bold**
    .replace(/__(.+?)__/g, "$1")            // __bold__
    .replace(/\*(.+?)\*/g, "$1")            // *italic*
    .replace(/_(.+?)_/g, "$1")              // _italic_
    .replace(/`{1,3}(.+?)`{1,3}/gs, "$1")  // `code` / ```code```
    .replace(/^#{1,6}\s+/gm, "")           // # headings
    .replace(/^\s*[-*+]\s+/gm, "")         // unordered lists
    .replace(/^\s*\d+\.\s+/gm, "")         // ordered lists
    .replace(/>\s?/g, "")                   // blockquotes
    .trim();
}

interface MessageRowProps {
  message: EventMessage;
}

const MessageRow: FC<MessageRowProps> = ({ message }) => (
  <div
    className="py-3 px-0 first:pt-0"
    style={{ borderBottomColor: "var(--color-border)" }}
  >
    {/* Channel + timestamp */}
    <div className="flex items-center justify-between mb-1.5 gap-2 flex-wrap">
      <span
        className="text-xs font-semibold uppercase tracking-wide"
        style={{ color: "var(--color-accent-blue)" }}
      >
        @{message.channel}
      </span>
      <span
        className="text-xs tabular-nums"
        style={{ color: "var(--color-text-secondary)" }}
      >
        {formatLocalTime(message.timestamp)}
      </span>
    </div>

    {/* Message text */}
    <p
      className="text-sm leading-relaxed mb-2"
      style={{ color: "var(--color-text-primary)" }}
    >
      {truncate(message.text, MAX_TEXT_CHARS)}
    </p>

    {/* Source link */}
    <a
      href={message.url}
      target="_blank"
      rel="noopener noreferrer"
      className="inline-flex items-center gap-1 text-xs transition-opacity hover:opacity-75"
      style={{ color: "var(--color-accent-blue)" }}
    >
      View original ↗
    </a>
  </div>
);

const IMPACT_COLORS: Record<ImpactLevel, string> = {
  low: "var(--color-text-secondary)",
  medium: "#d4a017",
  high: "#e8722a",
  critical: "#ef4444",
};

function formatEventType(t: string): string {
  return t.replace(/_/g, " ");
}

const EventCard: FC<EventCardProps> = ({ event, isNew = false }) => {
  const [expanded, setExpanded] = useState(false);

  const visibleMessages = expanded
    ? event.messages.slice(0, MAX_MESSAGES_SHOWN)
    : [];

  const timeRange =
    event.first_seen === event.last_seen
      ? formatLocalTime(event.first_seen)
      : `${formatLocalTime(event.first_seen)} → ${formatLocalTime(event.last_seen)}`;

  const summaryText = event.summary ? stripMarkdown(event.summary) : "";
  const impactColor = IMPACT_COLORS[event.impact_level] ?? IMPACT_COLORS.low;

  return (
    <article
      className={[
        "rounded-lg border hover:shadow-lg",
        isNew ? "event-card-new" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      style={{
        backgroundColor: "var(--color-bg-card)",
        borderColor: "var(--color-border)",
      }}
    >
      {/* ── Collapsed header (always visible) ── */}
      <div className="p-4">
        {/* Top row: confidence badge + source count */}
        <div className="flex items-center justify-between mb-2 gap-2">
          <div className="flex items-center gap-3">
            <ConfidenceBadge confidence={event.confidence} />
            <span
              className="text-xs"
              style={{ color: "var(--color-text-secondary)" }}
            >
              {event.source_count}{" "}
              {event.source_count === 1 ? "source" : "sources"}
            </span>
          </div>

          {/* Expand / collapse button */}
          <button
            onClick={() => setExpanded((prev) => !prev)}
            className="text-xs px-2.5 py-1 rounded border transition-colors duration-100 cursor-pointer"
            style={{
              borderColor: "var(--color-border)",
              color: "var(--color-text-secondary)",
            }}
            aria-expanded={expanded}
            aria-label={expanded ? "Collapse event" : "Expand event"}
          >
            {expanded ? "Collapse ▲" : "Expand ▼"}
          </button>
        </div>

        {/* Title */}
        <h2
          className="text-sm font-semibold leading-snug mb-2 line-clamp-2"
          style={{ color: "var(--color-text-primary)" }}
        >
          {event.title}
        </h2>

        {/* Summary — plain text, no markdown, no dangerouslySetInnerHTML */}
        {summaryText && (
          <p
            className="text-xs leading-relaxed mb-3"
            style={{ color: "var(--color-text-secondary)" }}
          >
            {summaryText}
          </p>
        )}

        {/* Time range + metadata */}
        <div className="flex flex-wrap items-center gap-2 mb-3">
          <span
            className="text-xs tabular-nums"
            style={{ color: "var(--color-text-secondary)" }}
          >
            {timeRange}
          </span>
          {event.event_type && event.event_type !== "unknown" && (
            <span
              className="text-xs px-2 py-0.5 rounded-full border font-medium uppercase tracking-wide"
              style={{
                borderColor: "var(--color-border)",
                color: "var(--color-accent-blue)",
                backgroundColor: "var(--color-bg-base)",
              }}
            >
              {formatEventType(event.event_type)}
            </span>
          )}
          {event.impact_level && event.impact_level !== "low" && (
            <span
              className="text-xs px-2 py-0.5 rounded-full border font-medium uppercase tracking-wide"
              style={{
                borderColor: impactColor,
                color: impactColor,
                backgroundColor: "var(--color-bg-base)",
              }}
            >
              {event.impact_level}
            </span>
          )}
        </div>

        {/* Channels */}
        <div className="flex flex-wrap gap-1.5">
          {event.channels.map((ch) => (
            <span
              key={ch}
              className="text-xs px-2 py-0.5 rounded-full border"
              style={{
                borderColor: "var(--color-border)",
                color: "var(--color-text-secondary)",
                backgroundColor: "var(--color-bg-base)",
              }}
            >
              @{ch}
            </span>
          ))}
        </div>
      </div>

      {/* ── Expanded messages ── */}
      {expanded && visibleMessages.length > 0 && (
        <div
          className="border-t px-4 pt-3 pb-4 space-y-0 divide-y"
          style={{
            borderTopColor: "var(--color-border)",
            "--tw-divide-opacity": "1",
          } as React.CSSProperties}
        >
          {visibleMessages.map((msg) => (
            <MessageRow
              key={`${msg.channel}-${msg.timestamp}`}
              message={msg}
            />
          ))}

          {event.messages.length > MAX_MESSAGES_SHOWN && (
            <p
              className="pt-3 text-xs"
              style={{ color: "var(--color-text-secondary)" }}
            >
              +{event.messages.length - MAX_MESSAGES_SHOWN} more message
              {event.messages.length - MAX_MESSAGES_SHOWN > 1 ? "s" : ""}
            </p>
          )}
        </div>
      )}
    </article>
  );
};

export default EventCard;
