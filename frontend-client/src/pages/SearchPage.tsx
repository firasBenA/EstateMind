/**
 * frontend-client/src/pages/SearchPage.tsx
 *
 * Listings search page — all filtering, sorting and pagination
 * is delegated to the Django backend via useListings().
 * The component only manages UI state and derives API params from it.
 */
import { useState, useEffect, useCallback } from "react";
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
import type { ListingFilters } from "@/lib/api";
import {
  X, LayoutGrid, List, SlidersHorizontal,
  Search, Building2, AlertCircle, ChevronLeft, ChevronRight,
} from "lucide-react";

// ── Constants ─────────────────────────────────────────────────────────────────
const PAGE_SIZE   = 24;
const PROP_TYPES  = ["apartment", "house", "land", "commercial"] as const;
const PROP_LABELS: Record<string, string> = {
  apartment: "Appartement", house: "Villa", land: "Terrain", commercial: "Commercial",
};
const ROOM_OPTIONS = ["Studio", "S+1", "S+2", "S+3", "S+4", "S+5+"];
const SORT_OPTIONS = [
  { value: "recent",       label: "Plus récents" },
  { value: "price_asc",    label: "Prix croissant" },
  { value: "price_desc",   label: "Prix décroissant" },
  { value: "price_m2_asc", label: "Prix/m² croissant" },
];

// ── Helpers ───────────────────────────────────────────────────────────────────
function roomToRange(r: string): { min?: number; max?: number } {
  if (r === "Studio") return { min: 0, max: 0 };
  if (r === "S+5+")   return { min: 5 };
  const n = parseInt(r.replace("S+", ""), 10);
  return { min: n, max: n };
}

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
  const [searchParams, setSearchParams] = useSearchParams();

  // ── UI state ─────────────────────────────────────────────────────────────
  const [searchText,   setSearchText]   = useState(searchParams.get("q") || "");
  const [city,         setCity]         = useState(searchParams.get("city") || "");
  const [transType,    setTransType]    = useState(searchParams.get("transaction") || "all");
  const [propTypes,    setPropTypes]    = useState<string[]>(
    searchParams.get("type") ? [searchParams.get("type")!] : [],
  );
  const [priceRange,   setPriceRange]   = useState([0, 5_000_000]);
  const [surfaceRange, setSurfaceRange] = useState([0, 1_000]);
  const [rooms,        setRooms]        = useState<string[]>([]);
  const [showFlagged,  setShowFlagged]  = useState(false);
  const [sortBy,       setSortBy]       = useState<ListingFilters["sort"]>("recent");
  const [viewMode,     setViewMode]     = useState<"grid" | "list">("grid");
  const [page,         setPage]         = useState(1);
  const [filtersOpen,  setFiltersOpen]  = useState(false);

  // ── Meta (cities from API) ───────────────────────────────────────────────
  const { meta } = useListingsMeta();
  const CITIES  = meta?.cities  ?? [];

  // ── Build API params from UI state ───────────────────────────────────────
  const roomRange = rooms.length === 1 ? roomToRange(rooms[0]) : {};
  const filters: ListingFilters = {
    page,
    page_size: PAGE_SIZE,
    ...(searchText   ? { q: searchText }             : {}),
    ...(city         ? { city }                      : {}),
    ...(transType !== "all" ? { transaction: transType as "sale" | "rent" } : {}),
    ...(propTypes.length === 1 ? { type: propTypes[0] } : {}),
    ...(priceRange[0] > 0           ? { min_price:   priceRange[0] }   : {}),
    ...(priceRange[1] < 5_000_000   ? { max_price:   priceRange[1] }   : {}),
    ...(surfaceRange[0] > 0         ? { min_surface: surfaceRange[0] } : {}),
    ...(surfaceRange[1] < 1_000     ? { max_surface: surfaceRange[1] } : {}),
    ...(roomRange.min !== undefined ? { min_rooms:   roomRange.min }   : {}),
    ...(roomRange.max !== undefined ? { max_rooms:   roomRange.max }   : {}),
    ...(showFlagged ? { fraud: true } : {}),
    sort: sortBy,
  };

  const { listings, total, pages, loading, error } = useListings(filters);

  // Reset page when filters change
  const resetPage = useCallback(() => setPage(1), []);
  useEffect(() => { resetPage(); }, [
    searchText, city, transType, propTypes, priceRange,
    surfaceRange, rooms, showFlagged, sortBy, resetPage,
  ]);

  // ── Active filter pills ──────────────────────────────────────────────────
  const activeFilters: { label: string; clear: () => void }[] = [];
  if (city)            activeFilters.push({ label: city,       clear: () => setCity("") });
  if (transType !== "all") activeFilters.push({ label: transType === "sale" ? "Vente" : "Location", clear: () => setTransType("all") });
  propTypes.forEach(t => activeFilters.push({ label: PROP_LABELS[t] ?? t, clear: () => setPropTypes(p => p.filter(x => x !== t)) }));
  rooms.forEach(r     => activeFilters.push({ label: r, clear: () => setRooms(p => p.filter(x => x !== r)) }));

  const togglePropType = (t: string) =>
    setPropTypes(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t]);
  const toggleRoom = (r: string) =>
    setRooms(prev => prev.includes(r) ? prev.filter(x => x !== r) : [...prev, r]);

  // ── Filters panel (shared desktop/mobile) ───────────────────────────────
  const FiltersPanel = (
    <div className="space-y-6">
      {/* Search */}
      <div>
        <Label className="text-sm font-medium mb-2 block">Recherche</Label>
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            className="pl-8"
            placeholder="Titre, description…"
            value={searchText}
            onChange={e => setSearchText(e.target.value)}
          />
        </div>
      </div>

      {/* City */}
      <div>
        <Label className="text-sm font-medium mb-2 block">Ville</Label>
        <Select value={city || "all"} onValueChange={v => setCity(v === "all" ? "" : v)}>
          <SelectTrigger><SelectValue placeholder="Toutes les villes" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Toutes les villes</SelectItem>
            {CITIES.map(c => <SelectItem key={c} value={c}>{c}</SelectItem>)}
          </SelectContent>
        </Select>
      </div>

      {/* Transaction */}
      <div>
        <Label className="text-sm font-medium mb-2 block">Type de transaction</Label>
        <div className="flex gap-2">
          {[["all","Tous"],["sale","Vente"],["rent","Location"]].map(([v, l]) => (
            <Button key={v} size="sm" variant={transType === v ? "default" : "outline"}
              onClick={() => setTransType(v)} className="flex-1 text-xs">
              {l}
            </Button>
          ))}
        </div>
      </div>

      {/* Property type */}
      <div>
        <Label className="text-sm font-medium mb-2 block">Type de bien</Label>
        <div className="space-y-2">
          {PROP_TYPES.map(t => (
            <div key={t} className="flex items-center gap-2">
              <Checkbox checked={propTypes.includes(t)} onCheckedChange={() => togglePropType(t)} id={`pt-${t}`} />
              <label htmlFor={`pt-${t}`} className="text-sm cursor-pointer">{PROP_LABELS[t]}</label>
            </div>
          ))}
        </div>
      </div>

      {/* Price */}
      <div>
        <Label className="text-sm font-medium mb-3 block">
          Prix : {priceRange[0].toLocaleString("fr-TN")} – {priceRange[1].toLocaleString("fr-TN")} TND
        </Label>
        <Slider min={0} max={5_000_000} step={10_000}
          value={priceRange} onValueChange={setPriceRange} />
      </div>

      {/* Surface */}
      <div>
        <Label className="text-sm font-medium mb-3 block">
          Surface : {surfaceRange[0]} – {surfaceRange[1]} m²
        </Label>
        <Slider min={0} max={1_000} step={10}
          value={surfaceRange} onValueChange={setSurfaceRange} />
      </div>

      {/* Rooms */}
      <div>
        <Label className="text-sm font-medium mb-2 block">Pièces</Label>
        <div className="flex flex-wrap gap-1.5">
          {ROOM_OPTIONS.map(r => (
            <Button key={r} size="sm" variant={rooms.includes(r) ? "default" : "outline"}
              onClick={() => toggleRoom(r)} className="text-xs">
              {r}
            </Button>
          ))}
        </div>
      </div>

      {/* Flagged */}
      <div className="flex items-center justify-between">
        <Label className="text-sm">Inclure les annonces suspectes</Label>
        <Switch checked={showFlagged} onCheckedChange={setShowFlagged} />
      </div>

      {/* Reset */}
      {activeFilters.length > 0 && (
        <Button variant="ghost" size="sm" className="w-full text-muted-foreground"
          onClick={() => {
            setSearchText(""); setCity(""); setTransType("all"); setPropTypes([]);
            setPriceRange([0, 5_000_000]); setSurfaceRange([0, 1_000]);
            setRooms([]); setShowFlagged(false);
          }}>
          Réinitialiser les filtres
        </Button>
      )}
    </div>
  );

  // ── Pagination helper ─────────────────────────────────────────────────────
  const PaginationRow = pages > 1 ? (
    <div className="flex justify-center items-center gap-2 mt-8">
      <Button variant="outline" size="sm" disabled={page === 1}
        onClick={() => setPage(p => p - 1)}>
        <ChevronLeft className="h-4 w-4" />
      </Button>
      {Array.from({ length: Math.min(pages, 7) }, (_, i) => {
        const p = page > 4 ? page - 3 + i : i + 1;
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
                  {loading ? "Chargement…" : `${total.toLocaleString("fr-TN")} annonces`}
                </span>
                <Button variant="outline" size="sm" className="lg:hidden"
                  onClick={() => setFiltersOpen(v => !v)}>
                  <SlidersHorizontal className="h-4 w-4 mr-1" /> Filtres
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
                  <Badge key={i} variant="secondary" className="gap-1 capitalize pl-2.5">
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
                <Button variant="outline" size="sm" onClick={() => setPage(p => p)}>
                  Réessayer
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
                  <p className="font-semibold">Aucune annonce trouvée</p>
                  <p className="text-sm text-muted-foreground mt-1">Essayez d'élargir vos critères de recherche.</p>
                </div>
              </div>
            )}

            {/* Results grid */}
            {!loading && !error && listings.length > 0 && (
              <>
                {viewMode === "grid" ? (
                  <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
                    {listings.map(l => <ListingCard key={l.id} listing={l} />)}
                  </div>
                ) : (
                  <div className="space-y-3">
                    {listings.map(l => (
                      <Link key={l.id} to={`/listing/${l.id}`}
                        className="flex gap-4 border rounded-xl p-3 hover:bg-muted/50 transition-colors bg-card">
                        <img
                          src={(l.images?.[0] as { url: string })?.url ?? "/placeholder.svg"}
                          alt=""
                          className="w-32 h-24 object-cover rounded-lg shrink-0 bg-muted"
                        />
                        <div className="flex-1 min-w-0">
                          <h3 className="font-semibold text-sm truncate">{l.title}</h3>
                          <p className="text-xs text-muted-foreground mt-0.5">
                            {l.city}{l.zone ? ` • ${l.zone}` : ""} · {PROP_LABELS[l.type ?? ""] ?? l.type}
                          </p>
                          <p className="text-base font-bold text-primary mt-1">
                            {(l.price ?? 0).toLocaleString("fr-TN")} TND
                          </p>
                          {l.surface && (
                            <p className="text-xs text-muted-foreground">{l.surface} m²</p>
                          )}
                        </div>
                      </Link>
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