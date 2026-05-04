/**
 * frontend-client/src/lib/api.ts
 *
 * Typed API client for the EstateMind Django backend.
 * Handles CSRF token injection for all mutating requests.
 */
import { createClient } from "@supabase/supabase-js";
import { list } from "postcss";

export const supabase = createClient(
  import.meta.env.VITE_SUPABASE_URL ?? "https://amxnojlfczwffvtwutrb.supabase.co",
  import.meta.env.VITE_SUPABASE_ANON_KEY ?? "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFteG5vamxmY3p3ZmZ2dHd1dHJiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU3MjE3NDMsImV4cCI6MjA5MTI5Nzc0M30.hxj1C-NiJ2DSWK1p_63OgYtwX2uzjSLS1osMuek9Ow0",
);

const BASE_URL = import.meta.env.VITE_API_URL ?? "";

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

export interface Governorate {
  id: number;
  name: string;
  name_ar: string;
  value: string;
  latitude: number | null;
  longitude: number | null;
}

export interface Delegation {
  id: number;
  governorate_id: number;
  name: string;
  name_ar: string;
  value: string;
  postal_code: string;
  latitude: number | null;
  longitude: number | null;
}

export interface TitleValidationResponse {
  valid: boolean;
  message: string;
  confidence: number;
  suggested_title?: string;
}

export interface DelegationMatchResponse {
  original: string;
  corrected: string | null;
  matched: boolean;
  confidence: number;
  delegation_id: number | null;
  latitude: number | null;
  longitude: number | null;
  governorate: string | null;
  message: string;
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
  transaction_type?: "Sale" | "Rent";
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
  agency_name?: string;
  matricule_fiscale?: string;
}

// ── Create Listing Payload ──────────────────────────────────────────────────
export interface CreateListingPayload {
  title: string;
  type: "apartment" | "house" | "land" | "commercial";
  transaction: "sale" | "rent";
  city: string;
  rooms: number;
  surface: number;
  price: number;
  description: string;
  images: string[];
  features: string[];
  poi: string[];
  latitude: number | null;
  longitude: number | null;
  governorate?: string;
  region?: string;
  zone?: string;
  municipality?: string;
}

export interface CreateListingResponse {
  success: boolean;
  listing_id: string;
  pois_found: number;
  reliability_score: number;
  reliability_level: "HIGH" | "GOOD" | "LOW" | "DROP";
  title_validated: boolean;
  title_confidence: number;
  predicted_price: number | null;
  price_range: {
    low: number | null;
    high: number | null;
  };
  auto_corrected?: boolean;
  auto_correct_info?: {
    original: string;
    corrected: string;
    confidence: number;
    message: string;
  };
  message: string;
}

// ── AI Description Payload ──────────────────────────────────────────────────
export interface GenerateDescriptionPayload {
  metadata: {
    property_type: string;
    transaction: string;
    city: string;
    surface_m2?: string;
    rooms?: string;
    bathrooms?: string;
    price?: string;
    furnished?: string;
  };
  images?: File[];
}

export interface GenerateDescriptionResponse {
  description: string;
  highlights: string[];
  tone: "professional" | "friendly" | "concise";
  warnings?: string[];
}

// ── Image Upload Response ───────────────────────────────────────────────────
export interface UploadImageResponse {
  urls: string[];
  errors?: { filename: string; error: string }[];
}

// ════════════════════════════════════════════════════════════════════════════
// PREDICTION / AI SCENARIO TYPES
// ════════════════════════════════════════════════════════════════════════════
export interface ScenarioInput {
  property_type: "Apartment" | "Villa" | "Land" | "Commercial" | "Other";
  surface: number;
  city: string;
  region: string;
  years: number;
  monthly_rent?: number;
  initial_price?: number;
}

export interface YearlyPrediction {
  year: number;
  price: number;
  cumulative_rent: number;
  total_value: number;
  roi: number;
  inflation: number;
}

export interface PredictionResult {
  initial_price: number;
  yearly_predictions: YearlyPrediction[];
  total_roi: number;
  final_value: number;
  confidence_score: number;
  model_used: string;
  factors: Record<string, any>;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  cached?: boolean;
}

export interface ModelStatusResponse {
  models: Record<string, boolean>;
  models_path: string;
}

export interface MacroForecastResponse {
  inflation_forecast: number[];
  years: number[];
}

export interface BasePricesResponse {
  base_prices: Record<string, number>;
  city_multipliers: Record<string, number>;
}

// ════════════════════════════════════════════════════════════════════════════
// PREDICTION API
// ════════════════════════════════════════════════════════════════════════════
export const predictionApi = {
  /**
   * Predict property investment scenario
   * POST /api/ai/predict/
   */
  async predict(scenario: ScenarioInput): Promise<ApiResponse<PredictionResult>> {
    return apiFetch<ApiResponse<PredictionResult>>("/api/ai/predict/", {
      method: "POST",
      body: JSON.stringify(scenario),
    });
  },

  /**
   * Compare multiple scenarios
   * POST /api/ai/compare/
   */
  async compare(scenarios: ScenarioInput[]): Promise<ApiResponse<{ comparison: PredictionResult[] }>> {
    return apiFetch<ApiResponse<{ comparison: PredictionResult[] }>>("/api/ai/compare/", {
      method: "POST",
      body: JSON.stringify({ scenarios }),
    });
  },

  /**
   * Get base prices by property type and city multipliers
   * GET /api/ai/base-prices/
   */
  async getBasePrices(): Promise<ApiResponse<BasePricesResponse>> {
    return apiFetch<ApiResponse<BasePricesResponse>>("/api/ai/base-prices/");
  },

  /**
   * Get macroeconomic forecast (inflation)
   * GET /api/ai/macro-forecast/
   */
  async getMacroForecast(): Promise<ApiResponse<MacroForecastResponse>> {
    return apiFetch<ApiResponse<MacroForecastResponse>>("/api/ai/macro-forecast/");
  },

  /**
   * Get model status (which models are loaded)
   * GET /api/ai/status/
   */
  async getModelStatus(): Promise<ApiResponse<ModelStatusResponse>> {
    return apiFetch<ApiResponse<ModelStatusResponse>>("/api/ai/status/");
  },
};

// ════════════════════════════════════════════════════════════════════════════
// LISTINGS API
// ════════════════════════════════════════════════════════════════════════════
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

  create(payload: CreateListingPayload): Promise<CreateListingResponse> {
    return apiFetch<CreateListingResponse>("/api/listings/create/", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },

  validateTitle(title: string): Promise<TitleValidationResponse> {
    return apiFetch<TitleValidationResponse>("/api/validate-title/", {
      method: "POST",
      body: JSON.stringify({ title }),
    });
  },

  getGovernorates(): Promise<Governorate[]> {
    return apiFetch<Governorate[]>("/api/governorates/");
  },

  getDelegations(governorateId: number): Promise<Delegation[]> {
    return apiFetch<Delegation[]>(`/api/governorates/${governorateId}/delegations/`);
  },

  autoCorrectDelegation(delegationName: string, governorateId?: number): Promise<DelegationMatchResponse> {
    return apiFetch<DelegationMatchResponse>("/api/delegation/autocorrect/", {
      method: "POST",
      body: JSON.stringify({ delegation_name: delegationName, governorate_id: governorateId }),
    });
  },

  getUserListings(): Promise<{ listings: Listing[]; total: number; active_count: number }> {
    return apiFetch("/api/user/listings/");
  },

  getUserLikes(): Promise<{ likes: any[]; total: number }> {
    return apiFetch("/api/user/likes/");
  },

  getUserStats(): Promise<{
    active_listings: number;
    active_change: string;
    total_views: number;
    views_change: string;
    total_likes: number;
    likes_change: string;
    messages: number;
    unread_messages: number;
    roi_estimate: number;
  }> {
    return apiFetch("/api/user/stats/");
  },

  getUserActivity(): Promise<{ activities: Array<{ text: string; time: string; type: string }> }> {
    return apiFetch("/api/user/activity/");
  },

  like(listingId: string): Promise<{ liked: boolean; like_count: number }> {
    return apiFetch(`/api/listings/${listingId}/like/`, { method: "POST" });
  },

  async generateDescription(
    payload: GenerateDescriptionPayload & { files?: File[] }
  ): Promise<GenerateDescriptionResponse> {
    if (payload.files && payload.files.length > 0) {
      const formData = new FormData();
      payload.files.forEach(file => {
        formData.append("images", file);
      });
      formData.append("metadata", JSON.stringify(payload.metadata));

      const res = await fetch(`${BASE_URL}/api/generate-description/`, {
        method: "POST",
        credentials: "include",
        body: formData,
      });

      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.error || `AI Generation failed: ${res.status}`);
      }

      return res.json();
    }

    console.warn("Using mock description generator (no images provided)");
    await new Promise((resolve) => setTimeout(resolve, 1500));

    const { metadata } = payload;
    const type = metadata.property_type;
    const city = metadata.city;
    const surface = metadata.surface_m2 || "spacious";
    const rooms = metadata.rooms || "multiple";
    const transaction = metadata.transaction;

    const description = `Beautiful ${type} located in ${city}. This ${surface} m² property features ${rooms} rooms with modern finishes. Perfect for ${transaction === "rent" ? "tenants" : "buyers"}.`;

    return {
      description,
      highlights: ["modern finishes", "great location"],
      tone: "professional",
    };
  },

  async predictPrice(payload: {
    transaction: string;
    type: string;
    city: string;
    surface: number;
    rooms: number;
    images_count: number;
    has_description: number;
    desc_length: number;
    has_coords: number;
  }): Promise<any> {
    return apiFetch<any>("/api/predict-price/", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },
};

// ════════════════════════════════════════════════════════════════════════════
// LOCATIONS API
// ════════════════════════════════════════════════════════════════════════════
export const locationsApi = {
  getGovernorates(): Promise<Governorate[]> {
    return apiFetch<Governorate[]>("/api/governorates/");
  },

  getDelegations(governorateId: number): Promise<Delegation[]> {
    return apiFetch<Delegation[]>(`/api/governorates/${governorateId}/delegations/`);
  },

  autoCorrectDelegation(delegationName: string, governorateId?: number): Promise<DelegationMatchResponse> {
    return apiFetch<DelegationMatchResponse>("/api/delegation/autocorrect/", {
      method: "POST",
      body: JSON.stringify({ delegation_name: delegationName, governorate_id: governorateId }),
    });
  },
};

// ════════════════════════════════════════════════════════════════════════════
// STORAGE API (Supabase)
// ════════════════════════════════════════════════════════════════════════════
export const storageApi = {
  async uploadImages(files: File[]): Promise<UploadImageResponse> {
    const urls: string[] = [];
    const errors: { filename: string; error: string }[] = [];

    for (const file of files) {
      const fileExt = file.name.split(".").pop();
      const fileName = `${crypto.randomUUID()}.${fileExt}`;
      const filePath = `listings/${fileName}`;

      const { error, data } = await supabase.storage
        .from("property-images")
        .upload(filePath, file, {
          cacheControl: "3600",
          upsert: false,
        });

      if (error) {
        errors.push({ filename: file.name, error: error.message });
        continue;
      }

      const {
        data: { publicUrl },
      } = supabase.storage.from("property-images").getPublicUrl(filePath);
      urls.push(publicUrl);
    }

    return { urls, errors: errors.length > 0 ? errors : undefined };
  },

  async removeImage(url: string): Promise<boolean> {
    try {
      const path = url.split("/property-images/")[1];
      if (!path) return false;

      const { error } = await supabase.storage
        .from("property-images")
        .remove([path]);

      return !error;
    } catch {
      return false;
    }
  },
};

// ════════════════════════════════════════════════════════════════════════════
// AUTH API
// ════════════════════════════════════════════════════════════════════════════
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