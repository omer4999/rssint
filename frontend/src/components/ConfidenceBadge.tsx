import type { FC } from "react";

interface ConfidenceBadgeProps {
  confidence: number;
}

interface BadgeStyle {
  dotColor: string;
  label: string;
  textColor: string;
}

function getBadgeStyle(confidence: number): BadgeStyle {
  if (confidence >= 0.8) {
    return {
      dotColor: "var(--color-accent-green)",
      label: `${Math.round(confidence * 100)}%`,
      textColor: "var(--color-accent-green)",
    };
  }
  if (confidence >= 0.5) {
    return {
      dotColor: "var(--color-accent-yellow)",
      label: `${Math.round(confidence * 100)}%`,
      textColor: "var(--color-accent-yellow)",
    };
  }
  return {
    dotColor: "var(--color-text-secondary)",
    label: `${Math.round(confidence * 100)}%`,
    textColor: "var(--color-text-secondary)",
  };
}

/**
 * Displays a coloured confidence indicator dot with a percentage label.
 * Green ≥ 80%, Yellow ≥ 50%, Gray otherwise.
 */
const ConfidenceBadge: FC<ConfidenceBadgeProps> = ({ confidence }) => {
  const { dotColor, label, textColor } = getBadgeStyle(confidence);

  return (
    <span className="inline-flex items-center gap-1.5">
      <span
        className="inline-block w-2 h-2 rounded-full flex-shrink-0"
        style={{ backgroundColor: dotColor }}
        aria-hidden="true"
      />
      <span
        className="text-xs font-mono font-medium tabular-nums"
        style={{ color: textColor }}
      >
        {label}
      </span>
    </span>
  );
};

export default ConfidenceBadge;
