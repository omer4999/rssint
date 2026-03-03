/**
 * Types mirroring the /events/latest API response schema.
 * Keep in sync with backend schemas.py EventClusterResponse / EventCard.
 */

export interface EventMessage {
  channel: string;
  timestamp: string;
  text: string;
  url: string;
}

export type EventType =
  | "military_strike"
  | "rocket_attack"
  | "terror_attack"
  | "explosion"
  | "armed_clash"
  | "political_statement"
  | "diplomatic_move"
  | "sanctions"
  | "protest"
  | "internal_unrest"
  | "infrastructure_damage"
  | "cyber_attack"
  | "unknown";

export type ImpactLevel = "low" | "medium" | "high" | "critical";

export interface Event {
  event_id: string;
  title: string;
  summary: string;
  event_type: EventType;
  actors: string[];
  locations: string[];
  impact_level: ImpactLevel;
  confidence: number;
  source_count: number;
  channels: string[];
  first_seen: string;
  last_seen: string;
  messages: EventMessage[];
}

export interface EventsApiResponse {
  total: number;
  window_minutes: number;
  events: Event[];
}

/** Possible values for the time window selector. */
export type WindowMinutes = 30 | 60 | 180 | 720;

export interface WindowOption {
  label: string;
  value: WindowMinutes;
}

export const WINDOW_OPTIONS: WindowOption[] = [
  { label: "30m", value: 30 },
  { label: "1h", value: 60 },
  { label: "3h", value: 180 },
  { label: "12h", value: 720 },
];
