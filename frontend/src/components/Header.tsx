import type { FC } from "react";
import { formatLocalDate } from "../utils/time";

interface HeaderProps {
  lastUpdated: Date | null;
}

const Header: FC<HeaderProps> = ({ lastUpdated }) => {
  return (
    <header
      style={{ borderBottomColor: "var(--color-border)" }}
      className="sticky top-0 z-50 border-b"
    >
      <div
        className="max-w-[900px] mx-auto px-4 py-3 flex items-center justify-between"
        style={{ backgroundColor: "var(--color-bg-base)" }}
      >
        {/* Left: brand + live indicator */}
        <div className="flex items-center gap-3">
          <span
            className="text-xl font-bold tracking-widest uppercase"
            style={{ color: "var(--color-text-primary)" }}
          >
            RSSINT
          </span>

          {/* Live dot */}
          <span className="flex items-center gap-1.5">
            <span
              className="live-dot inline-block w-2 h-2 rounded-full"
              style={{ backgroundColor: "var(--color-accent-red)" }}
            />
            <span
              className="text-xs font-medium uppercase tracking-wider"
              style={{ color: "var(--color-accent-red)" }}
            >
              Live
            </span>
          </span>
        </div>

        {/* Centre: subtitle */}
        <span
          className="hidden sm:block text-xs uppercase tracking-widest"
          style={{ color: "var(--color-text-secondary)" }}
        >
          Open Source Intelligence Feed
        </span>

        {/* Right: last updated (local time) */}
        <span
          className="text-xs tabular-nums"
          style={{ color: "var(--color-text-secondary)" }}
        >
          {lastUpdated ? (
            <>
              <span className="hidden sm:inline">Updated </span>
              {formatLocalDate(lastUpdated)}
            </>
          ) : (
            "—"
          )}
        </span>
      </div>
    </header>
  );
};

export default Header;
