import axios from "axios";
import type { DevelopmentsApiResponse } from "../types/development";

const apiClient = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL ?? "/api",
  timeout: 90_000,
  headers: { Accept: "application/json" },
});

export async function fetchDevelopments(): Promise<DevelopmentsApiResponse> {
  const response = await apiClient.get<DevelopmentsApiResponse>(
    "/developments",
  );
  return response.data;
}
