import axios, { type AxiosInstance } from "axios";

export interface HourlyBriefData {
  window_start: string;
  window_end: string;
  summary: string;
}

const apiClient: AxiosInstance = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL ?? "/api",
  timeout: 90_000,
  headers: { Accept: "application/json" },
});

export async function fetchHourlyBrief(): Promise<HourlyBriefData> {
  const response = await apiClient.get<HourlyBriefData>("/brief/hourly");
  return response.data;
}
