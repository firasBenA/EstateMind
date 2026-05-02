/**
 * frontend-client/src/pages/SearchPage.tsx
 * 
 * Listings search page — with behavior tracking for recommendations
 */
import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { PublicLayout }   from "@/components/PublicLayout";
import { ListingCard }    from "@/components/ListingCard";
import { Button }         from "@/components/ui/button";
import { Input }          from "@/components/ui/input";
import { Badge }          from "@/components/ui/badge";
import { Skeleton }       from "@/components/ui/skeleton";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Switch }   from "@/components/ui/switch";
import { Slider }   from "@/components/ui/slider";
import { Label }    from "@/components/ui/label";
import { useListings, useListingsMeta } from "@/hooks/useListings";
import { useBehaviorTracker } from "@/hooks/useBehaviorTracker.ts";
import { useAuth } from "@/lib/auth-context";
import type { ListingFilters } from "@/lib/api";
import {
  X, LayoutGrid, List, SlidersHorizontal,
  Search, Building2, AlertCircle, ChevronLeft, ChevronRight,
} from "lucide-react";

// ── Constants ─────────────────────────────────────────────────────────────────
const PAGE_SIZE   = 24;

// Exact database values
const PROP_TYPES = [
  { value: "Apartment", label: "Apartment" },
  { value: "Villa", label: "Villa" },
  { value: "Land", label: "Land" },
  { value: "Commercial", label: "Commercial" },
  { value: "Other", label: "Other" },
];

// Transaction types from DB
const TRANSACTION_TYPES = [
  { value: "Sale", label: "Sale" },
  { value: "Rent", label: "Rent" },
];

// Room mapping
const ROOM_OPTIONS = [
  { label: "Studio", value: "0", min: 0, max: 0 },
  { label: "S+1", value: "1", min: 1, max: 1 },
  { label: "S+2", value: "2", min: 2, max: 2 },
  { label: "S+3", value: "3", min: 3, max: 3 },
  { label: "S+4", value: "4", min: 4, max: 4 },
  { label: "S+5+", value: "5", min: 5, max: undefined },
];

const SORT_OPTIONS = [
  { value: "recent",       label: "Most Recent" },
  { value: "price_asc",    label: "Price: Low to High" },
  { value: "price_desc",   label: "Price: High to Low" },
  { value: "price_m2_asc", label: "Price/m²: Low to High" },
];

// ── Skeleton cards ────────────────────────────────────────────────────────────
function CardSkeleton() {
  return (
    <div className="rounded-xl border bg-card overflow-hidden">
      <Skeleton className="aspect-[4/3] w-full" />
      <div className="p-4 space-y-2">
        <Skeleton className="h-4 w-3/4" />
        <Skeleton className="h-3 w-1/2" />
        <Skeleton className="h-5 w-1/3" />
      </div>
    </div>
  );
}

// ── Main component ─────────────────────────────────────────────────────────────
export default function SearchPage() {
  const { isAuthenticated } = useAuth();
  const { track, trackSearch } = useBehaviorTracker();
  const [searchParams, setSearchParams] = useSearchParams();

  // ── UI state ─────────────────────────────────────────────────────────────
  const [searchText,   setSearchText]   = useState(searchParams.get("q") || "");
  const [selectedCity, setSelectedCity] = useState(searchParams.get("city") || "");
  const [transType,    setTransType]    = useState<string>(searchParams.get("transaction") || "");
  const [selectedPropTypes, setSelectedPropTypes] = useState<string[]>(
    searchParams.get("type") ? [searchParams.get("type")!] : [],
  );
  const [priceRange,   setPriceRange]   = useState([0, 5_000_000]);
  const [surfaceRange, setSurfaceRange] = useState([0, 1_000]);
  const [selectedRooms, setSelectedRooms] = useState<string[]>([]);
  const [showFlagged,  setShowFlagged]  = useState(false);
  const [sortBy,       setSortBy]       = useState<ListingFilters["sort"]>("recent");
  const [viewMode,     setViewMode]     = useState<"grid" | "list">("grid");
  const [page,         setPage]         = useState(1);
  const [filtersOpen,  setFiltersOpen]  = useState(false);
  
  const lastSearchTrackedRef = useRef<string>("");

  // ── Meta (cities from API) - Remove duplicates ─────────────────────────────
  const { meta } = useListingsMeta();
  const uniqueCities = useMemo(() => {
    if (!meta?.cities) return [];
    return [...new Set(meta.cities)];
  }, [meta?.cities]);

  // ── Build API params from UI state ───────────────────────────────────────
  const getRoomRange = useCallback(() => {
    if (selectedRooms.length === 0) return {};
    
    let minRoom = Infinity;
    let maxRoom = -Infinity;
    
    for (const roomValue of selectedRooms) {
      const option = ROOM_OPTIONS.find(r => r.value === roomValue);
      if (option) {
        if (option.min !== undefined && option.min < minRoom) minRoom = option.min;
        if (option.max !== undefined && option.max > maxRoom) maxRoom = option.max;
        if (option.max === undefined) maxRoom = 10;
      }
    }
    
    if (minRoom === Infinity) return {};
    if (maxRoom === -Infinity) return { min_rooms: minRoom };
    if (minRoom === maxRoom) return { min_rooms: minRoom, max_rooms: maxRoom };
    return { min_rooms: minRoom, max_rooms: maxRoom };
  }, [selectedRooms]);
  
  const roomRange = getRoomRange();
  
  const filters: ListingFilters = {
    page,
    page_size: PAGE_SIZE,
    ...(searchText   ? { q: searchText }             : {}),
    ...(selectedCity ? { city: selectedCity }        : {}),
    ...(transType    ? { transaction_type: transType as "Sale" | "Rent" } : {}),
    ...(selectedPropTypes.length === 1 ? { type: selectedPropTypes[0] } : {}),
    ...(priceRange[0] > 0           ? { min_price:   priceRange[0] }   : {}),
    ...(priceRange[1] < 5_000_000   ? { max_price:   priceRange[1] }   : {}),
    ...(surfaceRange[0] > 0         ? { min_surface: surfaceRange[0] } : {}),
    ...(surfaceRange[1] < 1_000     ? { max_surface: surfaceRange[1] } : {}),
    ...(roomRange.min_rooms !== undefined ? { min_rooms: roomRange.min_rooms } : {}),
    ...(roomRange.max_rooms !== undefined ? { max_rooms: roomRange.max_rooms } : {}),
    ...(showFlagged ? { fraud: true } : {}),
    sort: sortBy,
  };

  const { listings, total, pages, loading, error } = useListings(filters);

  // Reset page when filters change
  const resetPage = useCallback(() => setPage(1), []);
  useEffect(() => { resetPage(); }, [
    searchText, selectedCity, transType, selectedPropTypes, priceRange,
    surfaceRange, selectedRooms, showFlagged, sortBy, resetPage,
  ]);

  // Sync URL with filters
  useEffect(() => {
    const params = new URLSearchParams();
    if (searchText) params.set("q", searchText);
    if (selectedCity) params.set("city", selectedCity);
    if (transType) params.set("transaction", transType);
    if (selectedPropTypes.length === 1) params.set("type", selectedPropTypes[0]);
    setSearchParams(params, { replace: true });
  }, [searchText, selectedCity, transType, selectedPropTypes]);

  // ── Track search when results load ──────────────────────────────────────
  useEffect(() => {
    if (!loading && !error && total > 0 && isAuthenticated) {
      const searchFingerprint = JSON.stringify({
        q: searchText,
        city: selectedCity,
        transaction: transType,
        types: selectedPropTypes,
        price_min: priceRange[0],
        price_max: priceRange[1],
        surface_min: surfaceRange[0],
        surface_max: surfaceRange[1],
        rooms: selectedRooms,
        sort: sortBy
      });
      
      if (searchFingerprint !== lastSearchTrackedRef.current) {
        trackSearch(
          searchText || "all listings",
          {
            city: selectedCity || undefined,
            transaction_type: transType || undefined,
            property_type: selectedPropTypes[0],
            min_price: priceRange[0] > 0 ? priceRange[0] : undefined,
            max_price: priceRange[1] < 5000000 ? priceRange[1] : undefined,
            min_surface: surfaceRange[0] > 0 ? surfaceRange[0] : undefined,
            max_surface: surfaceRange[1] < 1000 ? surfaceRange[1] : undefined,
            rooms: selectedRooms.length === 1 ? selectedRooms[0] : undefined
          },
          total,
          undefined
        );
        lastSearchTrackedRef.current = searchFingerprint;
      }
    }
  }, [loading, error, total, searchText, selectedCity, transType, selectedPropTypes, 
      priceRange, surfaceRange, selectedRooms, sortBy, isAuthenticated, trackSearch]);

  // ── Track click on listing ───────────────────────────────────────────────
  const handleListingClick = useCallback(async (listingId: string) => {
    if (isAuthenticated) {
      await track('search_click', listingId, {
        referrer: 'search_results',
        searchQuery: searchText || undefined,
        filters: {
          city: selectedCity || undefined,
          transaction_type: transType || undefined,
          property_type: selectedPropTypes[0]
        }
      });
    }
  }, [track, searchText, selectedCity, transType, selectedPropTypes, isAuthenticated]);

  // ── Active filter pills ──────────────────────────────────────────────────
  const activeFilters: { label: string; clear: () => void }[] = [];
  
  if (selectedCity) {
    activeFilters.push({ label: selectedCity, clear: () => setSelectedCity("") });
  }
  if (transType) {
    const transLabel = TRANSACTION_TYPES.find(t => t.value === transType)?.label || transType;
    activeFilters.push({ label: transLabel, clear: () => setTransType("") });
  }
  selectedPropTypes.forEach(t => {
    const propLabel = PROP_TYPES.find(p => p.value === t)?.label || t;
    activeFilters.push({ label: propLabel, clear: () => setSelectedPropTypes(p => p.filter(x => x !== t)) });
  });
  
  if (selectedRooms.length > 0) {
    let roomLabel = "";
    if (selectedRooms.length === 1) {
      roomLabel = ROOM_OPTIONS.find(o => o.value === selectedRooms[0])?.label || selectedRooms[0];
    } else {
      const labels = selectedRooms.map(r => ROOM_OPTIONS.find(o => o.value === r)?.label || r);
      roomLabel = labels.join(" + ");
    }
    activeFilters.push({ label: `Rooms: ${roomLabel}`, clear: () => setSelectedRooms([]) });
  }

  const togglePropType = (t: string) =>
    setSelectedPropTypes(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t]);
  
  const toggleRoom = (roomValue: string) => {
    setSelectedRooms(prev => {
      if (prev.includes(roomValue)) {
        return prev.filter(r => r !== roomValue);
      }
      return [...prev, roomValue];
    });
  };

  // ── Filters panel ───────────────────────────────────────────────────────
  const FiltersPanel = (
    <div className="space-y-6">
      {/* Search */}
      <div>
        <Label className="text-sm font-medium mb-2 block">Search</Label>
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            className="pl-8"
            placeholder="Title, description…"
            value={searchText}
            onChange={e => setSearchText(e.target.value)}
          />
        </div>
      </div>

      {/* City - No duplicates */}
      <div>
        <Label className="text-sm font-medium mb-2 block">City</Label>
        <Select value={selectedCity || "all"} onValueChange={v => setSelectedCity(v === "all" ? "" : v)}>
          <SelectTrigger><SelectValue placeholder="All cities" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All cities</SelectItem>
            {uniqueCities.map(c => (
              <SelectItem key={c} value={c}>{c}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Transaction */}
      <div>
        <Label className="text-sm font-medium mb-2 block">Transaction Type</Label>
        <div className="flex gap-2">
          <Button 
            size="sm" 
            variant={transType === "" ? "default" : "outline"}
            onClick={() => setTransType("")} 
            className="flex-1 text-xs"
          >
            All
          </Button>
          {TRANSACTION_TYPES.map(({ value, label }) => (
            <Button 
              key={value} 
              size="sm" 
              variant={transType === value ? "default" : "outline"}
              onClick={() => setTransType(transType === value ? "" : value)} 
              className="flex-1 text-xs"
            >
              {label}
            </Button>
          ))}
        </div>
      </div>

      {/* Property type */}
      <div>
        <Label className="text-sm font-medium mb-2 block">Property Type</Label>
        <div className="space-y-2">
          {PROP_TYPES.map(({ value, label }) => (
            <div key={value} className="flex items-center gap-2">
              <Checkbox 
                checked={selectedPropTypes.includes(value)} 
                onCheckedChange={() => togglePropType(value)} 
                id={`pt-${value}`} 
              />
              <label htmlFor={`pt-${value}`} className="text-sm cursor-pointer">{label}</label>
            </div>
          ))}
        </div>
        {selectedPropTypes.length > 1 && (
          <p className="text-xs text-muted-foreground mt-1">
            Note: Multiple types selected - search will return all these types
          </p>
        )}
      </div>

      {/* Price */}
      <div>
        <Label className="text-sm font-medium mb-3 block">
          Price: {priceRange[0].toLocaleString("en-US")} – {priceRange[1].toLocaleString("en-US")} TND
        </Label>
        <Slider 
          min={0} 
          max={5_000_000} 
          step={50_000}
          value={priceRange} 
          onValueChange={setPriceRange} 
        />
      </div>

      {/* Surface */}
      <div>
        <Label className="text-sm font-medium mb-3 block">
          Surface Area: {surfaceRange[0]} – {surfaceRange[1]} m²
        </Label>
        <Slider 
          min={0} 
          max={1_000} 
          step={20}
          value={surfaceRange} 
          onValueChange={setSurfaceRange} 
        />
      </div>

      {/* Rooms */}
      <div>
        <Label className="text-sm font-medium mb-2 block">Number of Rooms</Label>
        <div className="flex flex-wrap gap-1.5">
          <Button 
            size="sm" 
            variant={selectedRooms.length === 0 ? "default" : "outline"}
            onClick={() => setSelectedRooms([])} 
            className="text-xs"
          >
            All
          </Button>
          {ROOM_OPTIONS.map(({ value, label }) => (
            <Button 
              key={value} 
              size="sm" 
              variant={selectedRooms.includes(value) ? "default" : "outline"}
              onClick={() => toggleRoom(value)} 
              className="text-xs"
            >
              {label}
            </Button>
          ))}
        </div>
        {selectedRooms.length > 1 && (
          <p className="text-xs text-muted-foreground mt-1">
            Selected range: {selectedRooms.length} options
          </p>
        )}
      </div>

      {/* Flagged */}
      <div className="flex items-center justify-between">
        <Label className="text-sm">Include suspicious listings</Label>
        <Switch checked={showFlagged} onCheckedChange={setShowFlagged} />
      </div>

      {/* Reset */}
      {activeFilters.length > 0 && (
        <Button 
          variant="ghost" 
          size="sm" 
          className="w-full text-muted-foreground"
          onClick={() => {
            setSearchText(""); 
            setSelectedCity(""); 
            setTransType(""); 
            setSelectedPropTypes([]);
            setPriceRange([0, 5_000_000]); 
            setSurfaceRange([0, 1_000]);
            setSelectedRooms([]); 
            setShowFlagged(false);
          }}
        >
          Reset All Filters
        </Button>
      )}
    </div>
  );

  // ── Pagination ───────────────────────────────────────────────────────────
  const PaginationRow = pages > 1 ? (
    <div className="flex justify-center items-center gap-2 mt-8">
      <Button variant="outline" size="sm" disabled={page === 1}
        onClick={() => setPage(p => p - 1)}>
        <ChevronLeft className="h-4 w-4" />
      </Button>
      {Array.from({ length: Math.min(pages, 7) }, (_, i) => {
        let p = page;
        if (pages <= 7) p = i + 1;
        else if (page <= 4) p = i + 1;
        else if (page >= pages - 3) p = pages - 6 + i;
        else p = page - 3 + i;
        
        if (p < 1 || p > pages) return null;
        return (
          <Button key={p} size="sm" variant={p === page ? "default" : "outline"}
            onClick={() => setPage(p)}>{p}</Button>
        );
      })}
      <Button variant="outline" size="sm" disabled={page === pages}
        onClick={() => setPage(p => p + 1)}>
        <ChevronRight className="h-4 w-4" />
      </Button>
    </div>
  ) : null;

  return (
    <PublicLayout>
      <div className="container mx-auto px-4 py-6">
        <div className="flex gap-6">

          {/* Desktop sidebar */}
          <aside className="hidden lg:block w-72 shrink-0">
            <div className="sticky top-20 bg-card rounded-xl border p-4 max-h-[calc(100vh-6rem)] overflow-y-auto">
              {FiltersPanel}
            </div>
          </aside>

          {/* Main content */}
          <div className="flex-1 min-w-0">

            {/* Toolbar */}
            <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
              <div className="flex items-center gap-3">
                <span className="text-sm text-muted-foreground font-medium">
                  {loading ? "Loading…" : `${total.toLocaleString("en-US")} listings`}
                </span>
                <Button variant="outline" size="sm" className="lg:hidden"
                  onClick={() => setFiltersOpen(v => !v)}>
                  <SlidersHorizontal className="h-4 w-4 mr-1" /> Filters
                  {activeFilters.length > 0 && (
                    <Badge className="ml-1.5 h-4 w-4 p-0 flex items-center justify-center text-[10px]">
                      {activeFilters.length}
                    </Badge>
                  )}
                </Button>
              </div>

              <div className="flex items-center gap-2">
                <Select value={sortBy} onValueChange={v => setSortBy(v as typeof sortBy)}>
                  <SelectTrigger className="w-48">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {SORT_OPTIONS.map(o => (
                      <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <div className="flex border rounded-lg overflow-hidden">
                  <button className={`p-2 transition-colors ${viewMode === "grid" ? "bg-muted" : "hover:bg-muted/50"}`}
                    onClick={() => setViewMode("grid")}><LayoutGrid className="h-4 w-4" /></button>
                  <button className={`p-2 transition-colors ${viewMode === "list" ? "bg-muted" : "hover:bg-muted/50"}`}
                    onClick={() => setViewMode("list")}><List className="h-4 w-4" /></button>
                </div>
              </div>
            </div>

            {/* Mobile filters */}
            {filtersOpen && (
              <div className="lg:hidden mb-4 bg-card rounded-xl border p-4">{FiltersPanel}</div>
            )}

            {/* Active filter pills */}
            {activeFilters.length > 0 && (
              <div className="flex flex-wrap gap-2 mb-4">
                {activeFilters.map((f, i) => (
                  <Badge key={i} variant="secondary" className="gap-1 pl-2.5">
                    {f.label}
                    <button onClick={f.clear} className="ml-0.5 hover:text-destructive">
                      <X className="h-3 w-3" />
                    </button>
                  </Badge>
                ))}
              </div>
            )}

            {/* Error state */}
            {error && (
              <div className="flex flex-col items-center justify-center py-20 text-center gap-3">
                <AlertCircle className="h-10 w-10 text-destructive/60" />
                <p className="text-sm text-muted-foreground">{error}</p>
                <Button variant="outline" size="sm" onClick={() => window.location.reload()}>
                  Try Again
                </Button>
              </div>
            )}

            {/* Loading skeletons */}
            {loading && !error && (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
                {Array.from({ length: PAGE_SIZE }).map((_, i) => <CardSkeleton key={i} />)}
              </div>
            )}

            {/* Empty state */}
            {!loading && !error && listings.length === 0 && (
              <div className="flex flex-col items-center justify-center py-24 text-center gap-4">
                <Building2 className="h-14 w-14 text-muted-foreground/30" />
                <div>
                  <p className="font-semibold">No listings found</p>
                  <p className="text-sm text-muted-foreground mt-1">
                    {activeFilters.length > 0 
                      ? "Try broadening your search criteria."
                      : "No listings available at the moment."}
                  </p>
                </div>
              </div>
            )}

            {/* Results grid */}
            {!loading && !error && listings.length > 0 && (
              <>
                {viewMode === "grid" ? (
                  <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
                    {listings.map(l => (
                      <div 
                        key={l.id} 
                        onClick={() => handleListingClick(l.id)}
                        className="cursor-pointer"
                      >
                        <Link to={`/listing/${l.id}`}>
                          <ListingCard listing={l} />
                        </Link>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="space-y-3">
                    {listings.map(l => (
                      <div key={l.id} onClick={() => handleListingClick(l.id)}>
                        <Link to={`/listing/${l.id}`} className="flex gap-4 border rounded-xl p-3 hover:bg-muted/50 transition-colors bg-card">
                          <img
                            src={(l.images?.[0] as { url: string })?.url ?? "/no-image.svg"}
                            alt=""
                            className="w-32 h-24 object-cover rounded-lg shrink-0 bg-muted"
                          />
                          <div className="flex-1 min-w-0">
                            <h3 className="font-semibold text-sm truncate">{l.title}</h3>
                            <p className="text-xs text-muted-foreground mt-0.5">
                              {l.city}{l.zone ? ` • ${l.zone}` : ""} · {PROP_TYPES.find(p => p.value === l.type)?.label ?? l.type}
                            </p>
                            <p className="text-base font-bold text-primary mt-1">
                              {l.price ? l.price.toLocaleString("en-US") : "Price on request"} TND
                            </p>
                            {l.surface && (
                              <p className="text-xs text-muted-foreground">{l.surface} m²</p>
                            )}
                          </div>
                        </Link>
                      </div>
                    ))}
                  </div>
                )}
                {PaginationRow}
              </>
            )}

          </div>
        </div>
      </div>
    </PublicLayout>
  );
}