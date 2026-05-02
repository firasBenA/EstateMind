import { useQuery } from "@tanstack/react-query";

const BASE = "http://localhost:8000";

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { credentials: "include" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export function useMetrics() {
  return useQuery({
    queryKey: ["admin-metrics"],
    queryFn:  () => apiFetch<any>("/api/metrics/"),
    refetchInterval: 30_000,
  });
}

export function useEda() {
  return useQuery({
    queryKey: ["admin-eda"],
    queryFn:  () => apiFetch<any>("/api/eda/"),
    refetchInterval: 60_000,
  });
}

export function useQuality() {
  return useQuery({
    queryKey: ["admin-quality"],
    queryFn:  () => apiFetch<any>("/api/quality/"),
    refetchInterval: 60_000,
  });
}

export function useListingsAdmin(params: Record<string, any> = {}) {
  const qs = new URLSearchParams(
    Object.fromEntries(Object.entries(params).filter(([, v]) => v !== undefined && v !== ""))
  ).toString();
  return useQuery({
    queryKey: ["admin-listings", params],
    queryFn:  () => apiFetch<any>(`/api/listings/${qs ? `?${qs}` : ""}`),
    refetchInterval: 60_000,
  });
}