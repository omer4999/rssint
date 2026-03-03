import axios, { type AxiosInstance } from "axios";
import type { EventsApiResponse } from "../types/event";

/**
 * Dedicated Axios instance for the RSSINT backend.
 *
 * Base URL points to the Vite dev-server proxy (/api → http://127.0.0.1:8000).
 * In production, replace VITE_API_BASE_URL with the actual backend origin.
 */
const apiClient: AxiosInstance = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL ?? "/api",
  timeout: 90_000,
  headers: {
    Accept: "application/json",
  },
});

/**
 * Fetch the latest clustered intelligence events.
 *
 * @param minutes - Look-back window in minutes (30 / 60 / 180 / 720).
 *                  Maps to the backend query param `minutes`.
 * @returns Parsed EventsApiResponse from the backend.
 */
export async function fetchLatestEvents(
  minutes: number,
): Promise<EventsApiResponse> {
  const response = await apiClient.get<EventsApiResponse>("/events/latest", {
    params: { minutes, limit: 500 },
  });
  return response.data;
}
