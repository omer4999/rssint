import { useEffect, type FC } from "react";
import { useEvents } from "../hooks/useEvents";
import EventCard from "../components/EventCard";
import { formatLocalDate } from "../utils/time";

/** Feed always shows events from the last hour; no time filter. */
const FEED_WINDOW_MINUTES = 60;

interface FeedPageProps {
  /** Callback fired whenever a successful poll completes. */
  onLastUpdated: (date: Date) => void;
}

const FeedPage: FC<FeedPageProps> = ({ onLastUpdated }) => {
  const { events, total, loading, error, lastUpdated, newEventIds } =
    useEvents(FEED_WINDOW_MINUTES);

  useEffect(() => {
    if (lastUpdated) onLastUpdated(lastUpdated);
  }, [lastUpdated, onLastUpdated]);

  // True only on the very first load (no data yet).
  const isInitialLoad = loading && events.length === 0;
  // True when a background refresh is running while stale events are shown.
  const isRefreshing = loading && events.length > 0;

  return (
    <main className="max-w-[900px] mx-auto px-4 py-6">
      {/* ── Controls row ── */}
      <div className="flex items-center justify-between mb-6 gap-4 flex-wrap">
        {/* Status / event count */}
        <p className="text-sm flex items-center gap-2" style={{ color: "var(--color-text-secondary)" }}>
          {isInitialLoad ? (
            "Loading…"
          ) : error && events.length === 0 ? (
            <span style={{ color: "var(--color-accent-red)" }}>{error}</span>
          ) : (
            <>
              <span
                className="font-semibold"
                style={{ color: "var(--color-text-primary)" }}
              >
                {total}
              </span>{" "}
              event{total !== 1 ? "s" : ""} in the last hour
              {/* Inline "Refreshing" badge — shown immediately on window change */}
              {isRefreshing && (
                <span
                  className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full border"
                  style={{
                    borderColor: "var(--color-border)",
                    color: "var(--color-text-secondary)",
                    backgroundColor: "var(--color-bg-card)",
                  }}
                >
                  <span
                    className="live-dot inline-block w-1.5 h-1.5 rounded-full"
                    style={{ backgroundColor: "var(--color-accent-blue)" }}
                  />
                  Refreshing…
                </span>
              )}
            </>
          )}
        </p>
      </div>

      {/* ── Feed states ── */}
      {isInitialLoad && (
        <div
          className="text-center py-20 text-sm"
          style={{ color: "var(--color-text-secondary)" }}
        >
          Fetching intelligence feed…
        </div>
      )}

      {!loading && error && events.length === 0 && (
        <div
          className="text-center py-20 text-sm rounded-lg border p-6"
          style={{
            color: "var(--color-accent-red)",
            borderColor: "var(--color-border)",
            backgroundColor: "var(--color-bg-card)",
          }}
        >
          <p className="font-semibold mb-1">Connection error</p>
          <p style={{ color: "var(--color-text-secondary)" }}>{error}</p>
        </div>
      )}

      {!loading && !error && events.length === 0 && (
        <div
          className="text-center py-20 text-sm"
          style={{ color: "var(--color-text-secondary)" }}
        >
          No relevant events in the last hour.
        </div>
      )}

      {/* ── Event list — dims slightly while a background refresh runs ── */}
      {events.length > 0 && (
        <div
          className="flex flex-col gap-3 transition-opacity duration-300"
          style={{ opacity: isRefreshing ? 0.6 : 1 }}
        >
          {events.map((event) => (
            <EventCard
              key={event.event_id}
              event={event}
              isNew={newEventIds.has(event.event_id)}
            />
          ))}
        </div>
      )}

      {/* ── Footer ── */}
      {lastUpdated && (
        <p
          className="text-center text-xs mt-8"
          style={{ color: "var(--color-text-secondary)" }}
        >
          Auto-refreshes every 30s · Last update: {formatLocalDate(lastUpdated)}
        </p>
      )}
    </main>
  );
};

export default FeedPage;
