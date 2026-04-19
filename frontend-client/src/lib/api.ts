/**
 * frontend-client/src/lib/api.ts
 *
 * Typed API client for the EstateMind Django backend.
 * Handles CSRF token injection for all mutating requests.
 */

const BASE_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

// ── CSRF helper ───────────────────────────────────────────────────────────────
function getCsrfToken(): string {
  const name = "csrftoken";
  const match = document.cookie.match(new RegExp(`(^|;\\s*)${name}=([^;]*)`));
  return match ? decodeURIComponent(match[2]) : "";
}

// ── Base fetch ────────────────────────────────────────────────────────────────
async function apiFetch<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const method = (options.method ?? "GET").toUpperCase();
  const mutating = ["POST", "PUT", "PATCH", "DELETE"].includes(method);

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string>),
  };

  if (mutating) {
    headers["X-CSRFToken"] = getCsrfToken();
  }

  const res = await fetch(`${BASE_URL}${path}`, {
    ...options,
    credentials: "include",   // send session cookie + CSRF cookie
    headers,
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const message =
      (body as { error?: string; errors?: string[]; detail?: string })
        .error ??
      (body as { errors?: string[] }).errors?.join(" · ") ??
      (body as { detail?: string }).detail ??
      `HTTP ${res.status}`;
    throw new ApiError(message, res.status, body);
  }

  return res.json() as Promise<T>;
}

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public body: unknown,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

// ── Types ─────────────────────────────────────────────────────────────────────
export interface Listing {
  id: string;
  source_name: string;
  title: string;
  description: string | null;
  url: string | null;
  price: number | null;
  currency: string;
  transaction_type: "sale" | "rent" | null;
  type: "apartment" | "house" | "land" | "commercial" | null;
  rooms: number | null;
  city: string | null;
  municipality: string | null;
  zone: string | null;
  region: string | null;
  surface: number | null;
  price_per_m2: number | null;
  latitude: number | null;
  longitude: number | null;
  features: string[];
  images: { url: string; label: string }[];
  images_count: number;
  //fraud_flag: boolean;
  fraud_score: number | null;
  fraud_reason: string | null;
  reliability_score: number | null;
  reliability_level: "HIGH" | "MEDIUM" | "LOW" | null;
  is_outlier: boolean;
  outlier_flags: string[];
  suspected_duplicate: boolean;
  change_type: string | null;
  has_price_history: boolean;
  price_delta: number | null;
  price_delta_pct: number | null;
  scraped_at: string | null;
  last_updated: string | null;
  nlp_enriched: boolean;
  normalized: boolean;
  should_drop: boolean;
}

export interface ListingsResponse {
  count: number;
  pages: number;
  page: number;
  results: Listing[];
}

export interface ListingsMetaResponse {
  total_listings: number;
  cities_covered: number;
  avg_price_per_m2: number;
  listings_this_week: number;
  cities: string[];
  regions: string[];
}

export interface ListingFilters {
  page?: number;
  page_size?: number;
  q?: string;
  city?: string;
  region?: string;
  transaction?: "sale" | "rent";
  type?: string;
  min_price?: number;
  max_price?: number;
  min_surface?: number;
  max_surface?: number;
  min_rooms?: number;
  max_rooms?: number;
  fraud?: boolean;
  user_id?: string | number;
  sort?: "recent" | "price_asc" | "price_desc" | "price_m2_asc" | "price_m2_desc";
}

export interface SessionUser {
  is_authenticated: boolean;
  id: number;
  username: string;
  email: string;
  name: string;
  role: "particular" | "agency" | "analyst" | "admin";
  is_superuser: boolean;
  last_login: string | null;
}

export interface RegisterPayload {
  name: string;
  email: string;
  password: string;
  role: "particular" | "agency";
  date_of_birth: string;   // ISO: YYYY-MM-DD
  phone?: string;
  // agency only
  agency_name?: string;
  matricule_fiscale?: string;
}

// ── Listings API ──────────────────────────────────────────────────────────────
export const listingsApi = {
  list(filters: ListingFilters = {}): Promise<ListingsResponse> {
    const params = new URLSearchParams();
    (Object.entries(filters) as [string, unknown][]).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== "") {
        params.set(k, String(v));
      }
    });
    const qs = params.toString();
    return apiFetch<ListingsResponse>(`/api/listings/${qs ? `?${qs}` : ""}`);
  },

  get(id: string | number): Promise<Listing> {
    return apiFetch<Listing>(`/api/listings/${id}/`);
  },

  meta(): Promise<ListingsMetaResponse> {
    return apiFetch<ListingsMetaResponse>("/api/listings/meta/");
  },
};

// ── Auth API ──────────────────────────────────────────────────────────────────
export const authApi = {
  register(payload: RegisterPayload): Promise<SessionUser> {
    return apiFetch<SessionUser>("/api/register/", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },

  login(email: string, password: string): Promise<SessionUser> {
    return apiFetch<SessionUser>("/api/login/", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
  },

  logout(): Promise<void> {
    return apiFetch<void>("/api/logout/", { method: "POST" });
  },

  session(): Promise<SessionUser> {
    return apiFetch<SessionUser>("/api/session/");
  },
};