import { memo, type FC } from "react";
import { Handle, Position } from "@xyflow/react";

interface EventNodeData {
  title: string;
  impact_level: string;
  confidence: number;
  actors?: string[];
  first_seen?: string;
  borderColor: string;
  [key: string]: unknown;
}

const EventNode: FC<{ data: EventNodeData }> = ({ data }) => {
  return (
    <div
      className="rounded-lg border px-3 py-2.5"
      style={{
        backgroundColor: "var(--color-bg-card)",
        borderColor: data.borderColor,
        width: 300,
        borderWidth: 2,
      }}
    >
      <Handle type="target" id="target-left" position={Position.Left} style={handleStyle} />
      <Handle type="target" id="target-top" position={Position.Top} style={handleStyle} />
      <Handle type="target" id="target-right" position={Position.Right} style={handleStyle} />
      <Handle type="target" id="target-bottom" position={Position.Bottom} style={handleStyle} />
      <Handle type="source" id="source-right" position={Position.Right} style={handleStyle} />
      <Handle type="source" id="source-bottom" position={Position.Bottom} style={handleStyle} />
      <Handle type="source" id="source-top" position={Position.Top} style={handleStyle} />
      <Handle type="source" id="source-left" position={Position.Left} style={handleStyle} />

      <div
        className="text-xs font-bold leading-snug mb-1.5"
        style={{ color: "var(--color-text-primary)" }}
      >
        {data.title}
      </div>

      {(data.actors ?? []).length > 0 && (
        <div
          className="text-[10px] mb-1"
          style={{ color: "var(--color-text-secondary)" }}
        >
          {(data.actors ?? []).join(", ")}
        </div>
      )}

      <div className="flex flex-wrap items-center gap-1.5">
        <Badge color={data.borderColor} label={data.impact_level} />
        <span
          className="text-[10px] tabular-nums"
          style={{ color: "var(--color-text-secondary)" }}
        >
          {Math.round(data.confidence * 100)}%
        </span>
        {(data.first_seen ?? "") && (
          <span
            className="text-[10px] tabular-nums"
            style={{ color: "var(--color-text-secondary)" }}
          >
            {data.first_seen}
          </span>
        )}
      </div>
    </div>
  );
};

const handleStyle: React.CSSProperties = {
  background: "#4ea1ff",
  width: 7,
  height: 7,
  border: "none",
};

const Badge: FC<{ color: string; label: string }> = ({ color, label }) => (
  <span
    className="text-[10px] px-1.5 py-0.5 rounded-full border font-medium uppercase tracking-wide"
    style={{ borderColor: color, color }}
  >
    {label}
  </span>
);

export default memo(EventNode);
