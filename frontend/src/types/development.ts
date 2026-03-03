export interface DevEvent {
  id: string;
  title: string;
  impact_level: "medium" | "high" | "critical";
  confidence: number;
  first_seen: string;
  actors?: string[];
}

export interface DevEdge {
  source_event_id: string;
  target_event_id: string;
  relation_type: string;
}

export interface DevelopmentsApiResponse {
  events: DevEvent[];
  edges: DevEdge[];
}
