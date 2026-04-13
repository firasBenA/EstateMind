/**
 * frontend-client/src/hooks/useListings.ts
 *
 * Fetches paginated listings from /api/listings/ with full filter support.
 * Falls back to stale data while re-fetching (keeps UI responsive).
 */
import { useState, useEffect, useRef, useCallback } from "react";
import { listingsApi, type Listing, type ListingFilters, type ListingsMetaResponse } from "@/lib/api";

// ── Listings list ─────────────────────────────────────────────────────────────
export interface UseListingsState {
  listings:    Listing[];
  total:       number;
  pages:       number;
  page:        number;
  loading:     boolean;
  error:       string | null;
  refetch:     () => void;
}

export function useListings(filters: ListingFilters = {}): UseListingsState {
  const [listings, setListings] = useState<Listing[]>([]);
  const [total,    setTotal]    = useState(0);
  const [pages,    setPages]    = useState(1);
  const [page,     setPage]     = useState(1);
  const [loading,  setLoading]  = useState(true);
  const [error,    setError]    = useState<string | null>(null);
  const [tick,     setTick]     = useState(0);

  // Stable serialized key so the effect re-runs when filters change
  const filterKey = JSON.stringify(filters);
  const latestKey = useRef(filterKey);

  useEffect(() => {
    latestKey.current = filterKey;
    let cancelled = false;
    setLoading(true);
    setError(null);

    listingsApi.list(filters)
      .then(data => {
        if (cancelled || latestKey.current !== filterKey) return;
        setListings(data.results);
        setTotal(data.count);
        setPages(data.pages);
        setPage(data.page);
      })
      .catch(err => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Erreur de chargement.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterKey, tick]);

  const refetch = useCallback(() => setTick(t => t + 1), []);

  return { listings, total, pages, page, loading, error, refetch };
}

// ── Single listing ─────────────────────────────────────────────────────────────
export function useListing(id: number | null) {
  const [listing, setListing] = useState<Listing | null>(null);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState<string | null>(null);

  useEffect(() => {
    if (id === null) return;
    let cancelled = false;
    setLoading(true);
    setError(null);

    listingsApi.get(id)
      .then(data => { if (!cancelled) setListing(data); })
      .catch(err  => { if (!cancelled) setError(err instanceof Error ? err.message : "Erreur."); })
      .finally(()  => { if (!cancelled) setLoading(false); });

    return () => { cancelled = true; };
  }, [id]);

  return { listing, loading, error };
}

// ── Meta (cities, stats) ──────────────────────────────────────────────────────
export function useListingsMeta() {
  const [meta,    setMeta]    = useState<ListingsMetaResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState<string | null>(null);

  useEffect(() => {
    listingsApi.meta()
      .then(setMeta)
      .catch(err => setError(err instanceof Error ? err.message : "Erreur."))
      .finally(() => setLoading(false));
  }, []);

  return { meta, loading, error };
}