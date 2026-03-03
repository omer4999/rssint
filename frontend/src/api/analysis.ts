import axios from "axios";

const apiClient = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL ?? "/api",
  timeout: 30_000,
  headers: { Accept: "application/json" },
});

export interface AnalysisResponse {
  conflict_overview: string;
  latest_developments: string;
  possible_outcomes: string;
  generated_at: string;
}

export async function fetchAnalysis(): Promise<AnalysisResponse> {
  const response = await apiClient.get<AnalysisResponse>("/analysis");
  return response.data;
}
