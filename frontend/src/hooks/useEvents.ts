import { useState, useEffect, useCallback, useRef } from "react";
import { fetchLatestEvents } from "../api/events";
import type { Event } from "../types/event";

const POLL_INTERVAL_MS = 30_000;

export interface UseEventsResult {
  events: Event[];
  total: number;
  loading: boolean;
  error: string | null;
  lastUpdated: Date | null;
  /** IDs of events that were absent in the previous successful fetch. */
  newEventIds: Set<string>;
  refresh: () => void;
}

/**
 * Polls /events/latest every 30 seconds.
 *
 * Sorting:   events are ordered by `first_seen` DESC so the earliest-reported
 *            events appear at the top of the feed.
 *
 * New-event detection:
 *   After the first successful load for a given window, each subsequent
 *   fetch compares the incoming event IDs against the previous set.
 *   Any ID that is genuinely new is added to `newEventIds` so the UI can
 *   apply an entrance animation.  The initial load never highlights anything
 *   (everything would glow on the first render, which looks wrong).
 *
 * Stale-response guard:
 *   A monotonically incrementing `requestId` is stamped on every call.
 *   Responses that arrive after a newer request has been dispatched are
 *   silently dropped, preventing a slow earlier request from overwriting the
 *   result of a faster later one.
 */
export function useEvents(windowMinutes: number): UseEventsResult {
  const [events, setEvents] = useState<Event[]>([]);
  const [total, setTotal] = useState<number>(0);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [newEventIds, setNewEventIds] = useState<Set<string>>(new Set());

  const prevEventIdsRef = useRef<Set<string>>(new Set());
  const hasLoadedOnceRef = useRef<boolean>(false);
  const requestIdRef = useRef<number>(0);

  const load = useCallback(async () => {
    const myId = ++requestIdRef.current;

    try {
      const data = await fetchLatestEvents(windowMinutes);

      // Drop stale responses — a newer request has already superseded this one.
      if (myId !== requestIdRef.current) return;

      // Sort newest-first_seen first so recently reported events rise to the top.
      const sorted = [...data.events].sort(
        (a, b) =>
          new Date(b.first_seen).getTime() - new Date(a.first_seen).getTime(),
      );

      const currentIds = new Set(sorted.map((e) => e.event_id));

      // Only highlight events on subsequent fetches — not on the initial load.
      const newIds = hasLoadedOnceRef.current
        ? new Set([...currentIds].filter((id) => !prevEventIdsRef.current.has(id)))
        : new Set<string>();

      hasLoadedOnceRef.current = true;
      prevEventIdsRef.current = currentIds;

      setEvents(sorted);
      setTotal(data.total);
      setNewEventIds(newIds);
      setLastUpdated(new Date());
      setError(null);
    } catch (err) {
      if (myId !== requestIdRef.current) return;
      const message =
        err instanceof Error ? err.message : "Failed to fetch events.";
      setError(message);
    } finally {
      if (myId === requestIdRef.current) setLoading(false);
    }
  }, [windowMinutes]);

  useEffect(() => {
    // Reset per-window tracking so switching windows never highlights
    // every existing event as "new".
    hasLoadedOnceRef.current = false;
    prevEventIdsRef.current = new Set();
    setNewEventIds(new Set());

    setLoading(true);
    load();

    const id = setInterval(load, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [windowMinutes, load]);

  return { events, total, loading, error, lastUpdated, newEventIds, refresh: load };
}
