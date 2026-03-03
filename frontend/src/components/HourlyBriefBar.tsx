import { useState, useEffect, useCallback, type FC } from "react";
import { fetchHourlyBrief, type HourlyBriefData } from "../api/brief";
import { formatLocalTime } from "../utils/time";

const POLL_INTERVAL_MS = 3_600_000; // 1 hour

const HourlyBriefBar: FC = () => {
  const [brief, setBrief] = useState<HourlyBriefData | null>(null);
  const [collapsed, setCollapsed] = useState(false);
  const [error, setError] = useState(false);

  const load = useCallback(async () => {
    try {
      const data = await fetchHourlyBrief();
      setBrief(data);
      setError(false);
    } catch {
      setError(true);
    }
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [load]);

  const timeRange =
    brief
      ? `${formatLocalTime(brief.window_start)} – ${formatLocalTime(brief.window_end)}`
      : "";

  return (
    <div
      className="fixed bottom-0 left-0 right-0 z-50 transition-all duration-300 ease-in-out"
      style={{
        backgroundColor: "#161a21",
        borderTop: "1px solid #242a35",
        maxHeight: collapsed ? "48px" : "300px",
        overflow: "hidden",
      }}
    >
      {/* Header row — always visible */}
      <button
        onClick={() => setCollapsed((c) => !c)}
        className="w-full flex items-center justify-between px-6 py-3 cursor-pointer select-none"
        style={{ minHeight: "48px" }}
      >
        <div className="flex items-center gap-3">
          <span
            className="text-xs font-semibold uppercase tracking-wide"
            style={{ color: "var(--color-text-secondary)" }}
          >
            Last Hour Intelligence Summary
          </span>
          {timeRange && (
            <span className="text-xs" style={{ color: "var(--color-text-secondary)", opacity: 0.6 }}>
              {timeRange}
            </span>
          )}
        </div>
        <span
          className="text-xs transition-transform duration-200"
          style={{
            color: "var(--color-text-secondary)",
            transform: collapsed ? "rotate(180deg)" : "rotate(0deg)",
          }}
        >
          ▼
        </span>
      </button>

      {/* Body */}
      <div className="px-6 pb-4">
        {error && !brief && (
          <p className="text-sm" style={{ color: "var(--color-text-secondary)" }}>
            Summary temporarily unavailable.
          </p>
        )}
        {brief && (
          <p
            className="text-sm leading-relaxed"
            style={{ color: "var(--color-text-primary)" }}
          >
            {brief.summary}
          </p>
        )}
      </div>
    </div>
  );
};

export default HourlyBriefBar;
