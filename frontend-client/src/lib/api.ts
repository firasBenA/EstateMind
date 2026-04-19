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


const BASE_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8000";
// ── CSRF helper ───────────────────────────────────────────────────────────────
function getCsrfToken(): string {
  const name  = "csrftoken";
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
  id:                 number;
  source_name:        string;
  title:              string;
  description:        string | null;
  url:                string | null;
  price:              number | null;
  currency:           string;
  transaction_type:   "sale" | "rent" | null;
  type:               "apartment" | "house" | "land" | "commercial" | null;
  rooms:              number | null;
  city:               string | null;
  municipality:       string | null;
  zone:               string | null;
  region:             string | null;
  surface:            number | null;
  price_per_m2:       number | null;
  latitude:           number | null;
  longitude:          number | null;
  features:           string[];
  images:             { url: string; label: string }[];
  images_count:       number;
  fraud_flag:         boolean;
  fraud_score:        number | null;
  fraud_reason:       string | null;
  reliability_score:  number | null;
  reliability_level:  "HIGH" | "MEDIUM" | "LOW" | null;
  is_outlier:         boolean;
  outlier_flags:      string[];
  suspected_duplicate: boolean;
  change_type:        string | null;
  has_price_history:  boolean;
  price_delta:        number | null;
  price_delta_pct:    number | null;
  scraped_at:         string | null;
  last_updated:       string | null;
  nlp_enriched:       boolean;
  normalized:         boolean;
  should_drop:        boolean;
}

export interface ListingsResponse {
  count:   number;
  pages:   number;
  page:    number;
  results: Listing[];
}

export interface ListingsMetaResponse {
  total_listings:     number;
  cities_covered:     number;
  avg_price_per_m2:   number;
  listings_this_week: number;
  cities:             string[];
  regions:            string[];
}

export interface ListingFilters {
  page?:        number;
  page_size?:   number;
  q?:           string;
  city?:        string;
  region?:      string;
  transaction?: "sale" | "rent";
  type?:        string;
  min_price?:   number;
  max_price?:   number;
  min_surface?: number;
  max_surface?: number;
  min_rooms?:   number;
  max_rooms?:   number;
  fraud?:       boolean;
  sort?:        "recent" | "price_asc" | "price_desc" | "price_m2_asc" | "price_m2_desc";
}

export interface SessionUser {
  is_authenticated: boolean;
  id:          number;
  username:    string;
  email:       string;
  name:        string;
  role:        "particular" | "agency" | "analyst" | "admin";
  is_superuser: boolean;
  last_login:  string | null;
}

export interface RegisterPayload {
  name:              string;
  email:             string;
  password:          string;
  role:              "particular" | "agency";
  date_of_birth:     string;   // ISO: YYYY-MM-DD
  phone?:            string;
  // agency only
  agency_name?:      string;
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
  description?: string;
  images?: string[]; // Supabase URLs
  features?: string[];
  latitude?: number;
  longitude?: number;
  poi?: string[];
}

export interface CreateListingResponse {
  success: boolean;
  listing_id: string;
  reliability_score: number;
  reliability_level: "HIGH" | "GOOD" | "LOW" | "DROP";
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
  images?: File[]; // For future multimodal support
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

// ── Listings API ────────────────────────────────────────────────────────────
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

  get(id: number): Promise<Listing> {
    return apiFetch<Listing>(`/api/listings/${id}/`);
  },

  meta(): Promise<ListingsMetaResponse> {
    return apiFetch<ListingsMetaResponse>("/api/listings/meta/");
  },

  // ✅ CREATE NEW LISTING (User-submitted)
  create(payload: CreateListingPayload): Promise<CreateListingResponse> {
    return apiFetch<CreateListingResponse>("/api/listings/create/", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },

  async generateDescription(
    payload: GenerateDescriptionPayload & { files?: File[] } // Allow passing files
  ): Promise<GenerateDescriptionResponse> {

    // If files are provided, use the Real API via FormData
    if (payload.files && payload.files.length > 0) {
      const formData = new FormData();

      // Append images
      payload.files.forEach(file => {
        formData.append("images", file);
      });

      // Append metadata as JSON string
      formData.append("metadata", JSON.stringify(payload.metadata));

      // Call Django Proxy (which forwards to FastAPI)
      // Note: Do NOT set Content-Type header manually for FormData!
      const res = await fetch(`${BASE_URL}/api/generate-description/`, {
        method: "POST",
        credentials: "include", // Send CSRF cookie if needed by Django proxy
        body: formData,
      });

      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.error || `AI Generation failed: ${res.status}`);
      }

      return res.json();
    }

    // Fallback: Mock behavior if no files provided (for testing without backend)
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


// ── Storage API (Supabase) ──────────────────────────────────────────────────
export const storageApi = {
  // ✅ Upload images to Supabase Storage bucket "property-images"
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

  // ✅ Remove image from Supabase Storage (optional cleanup)
  async removeImage(url: string): Promise<boolean> {
    try {
      // Extract path from URL: https://.../property-images/listings/uuid.jpg
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

// ── Auth API ──────────────────────────────────────────────────────────────────
export const authApi = {
  register(payload: RegisterPayload): Promise<SessionUser> {
    return apiFetch<SessionUser>("/api/register/", {
      method: "POST",
      body:   JSON.stringify(payload),
    });
  },

  login(email: string, password: string): Promise<SessionUser> {
    return apiFetch<SessionUser>("/api/login/", {
      method: "POST",
      body:   JSON.stringify({ email, password }),
    });
  },

  logout(): Promise<void> {
    return apiFetch<void>("/api/logout/", { method: "POST" });
  },

  session(): Promise<SessionUser> {
    return apiFetch<SessionUser>("/api/session/");
  },
};

