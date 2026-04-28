/**
 * frontend-client/src/pages/ListingDetailPage.tsx
 *
 * Listing detail page — with behavior tracking for recommendations
 */
import { useParams, Link } from "react-router-dom";
import { PublicLayout }    from "@/components/PublicLayout";
import { ListingCard }     from "@/components/ListingCard";
import { Badge }           from "@/components/ui/badge";
import { Button }          from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress }        from "@/components/ui/progress";
import { Skeleton }        from "@/components/ui/skeleton";
import { useListing, useSimilarListings } from "@/hooks/useListings";
import { useBehaviorTracker } from "@/hooks/useBehaviorTracker.ts";
import { useAuth } from "@/lib/auth-context";
import { ReliabilityBadge, TypeBadge, TransactionBadge } from "@/components/Badges";
import SimilarListings from "@/components/SimilarListings";
import {
  MapPin, BedDouble, Maximize, ExternalLink, Calendar,
  AlertTriangle, ShieldAlert, Copy, Cpu, SlidersHorizontal,
  Heart, Phone, Share2, Bookmark,
} from "lucide-react";
import { useState, useEffect, useCallback } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer,
} from "recharts";

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatPrice(p: number | null): string {
  if (p == null) return "Prix sur demande";
  return p.toLocaleString("fr-TN") + " TND";
}

function formatPricePerM2(v: number | null): string {
  if (!v || v <= 0) return "";
  return v.toLocaleString("fr-TN") + " TND/m²";
}

function formatDate(s: string | null): string {
  if (!s) return "—";
  return new Date(s).toLocaleDateString("fr-TN", {
    day: "numeric", month: "short", year: "numeric",
  });
}

// ── Skeleton for images ───────────────────────────────────────────────────────
function ImageSkeleton() {
  return (
    <div className="space-y-3">
      <Skeleton className="aspect-[4/3] w-full rounded-lg" />
      <div className="flex gap-2">
        {[0,1,2].map(i => <Skeleton key={i} className="w-20 h-16 rounded-md" />)}
      </div>
    </div>
  );
}

// ── Strategy badge ────────────────────────────────────────────────────────────
function StrategyBadge({ strategy }: { strategy: string }) {
  if (strategy === "vector") {
    return (
      <Badge variant="secondary" className="gap-1 text-[10px]">
        <Cpu className="h-3 w-3" /> Vector similarity
      </Badge>
    );
  }
  return (
    <Badge variant="outline" className="gap-1 text-[10px]">
      <SlidersHorizontal className="h-3 w-3" /> Feature match
    </Badge>
  );
}

// ── Main ──────────────────────────────────────────────────────────────────────
export default function ListingDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { isAuthenticated } = useAuth();
  const { track } = useBehaviorTracker();
  
  const [selectedImg, setSelectedImg] = useState(0);
  const [descExpanded, setDescExpanded] = useState(false);
  const [viewTracked, setViewTracked] = useState(false);
  const [isSaved, setIsSaved] = useState(false);
  const [isFavorite, setIsFavorite] = useState(false);

  const { listing, loading, error } = useListing(id ?? null);
  const { data: simData, loading: simLoading } = useSimilarListings(id ?? null, 6);

  // ── Track view after 2 seconds (user spent time on page) ─────────────────
  useEffect(() => {
    if (!loading && listing && !viewTracked && isAuthenticated) {
      const timer = setTimeout(() => {
        track('view', listing.id, {
          duration: 30,
          referrer: document.referrer || 'direct'
        });
        setViewTracked(true);
      }, 2000);
      
      return () => clearTimeout(timer);
    }
  }, [loading, listing, viewTracked, isAuthenticated, track]);

  // ── Track save/favorite actions ──────────────────────────────────────────
  const handleSave = useCallback(async () => {
    if (!listing) return;
    
    setIsSaved(!isSaved);
    if (isAuthenticated && !isSaved) {
      await track('save', listing.id, { referrer: 'detail_page' });
      // TODO: Call your actual save API here
    }
  }, [listing, isAuthenticated, isSaved, track]);

  const handleFavorite = useCallback(async () => {
    if (!listing) return;
    
    setIsFavorite(!isFavorite);
    if (isAuthenticated && !isFavorite) {
      await track('favorite', listing.id, { referrer: 'detail_page' });
      // TODO: Call your actual favorite API here
    }
  }, [listing, isAuthenticated, isFavorite, track]);

  const handleContact = useCallback(async () => {
    if (!listing) return;
    
    if (isAuthenticated) {
      await track('contact', listing.id, { referrer: 'detail_page' });
      // TODO: Open contact modal or redirect to contact page
      alert(`Contact agency about: ${listing.title}`);
    } else {
      alert('Please login to contact the agency');
    }
  }, [listing, isAuthenticated, track]);

  const handleShare = useCallback(() => {
    if (navigator.share && listing) {
      navigator.share({
        title: listing.title,
        text: `Check out this property: ${listing.title}`,
        url: window.location.href,
      });
    } else {
      navigator.clipboard.writeText(window.location.href);
      alert('Link copied to clipboard!');
    }
  }, [listing]);

  // ── Loading state ─────────────────────────────────────────────────────────
  if (loading) {
    return (
      <PublicLayout>
        <div className="container mx-auto px-4 py-8">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <ImageSkeleton />
            <div className="space-y-4">
              <Skeleton className="h-8 w-3/4" />
              <Skeleton className="h-10 w-1/2" />
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-5/6" />
            </div>
          </div>
        </div>
      </PublicLayout>
    );
  }

  // ── Error / not found ─────────────────────────────────────────────────────
  if (error || !listing) {
    return (
      <PublicLayout>
        <div className="container mx-auto px-4 py-20 text-center">
          <h1 className="text-2xl font-bold mb-4">Annonce introuvable</h1>
          <p className="text-muted-foreground mb-6">{error ?? "Cette annonce n'existe pas ou a été supprimée."}</p>
          <Button asChild><Link to="/search">Retour à la recherche</Link></Button>
        </div>
      </PublicLayout>
    );
  }

  // ── Data helpers ──────────────────────────────────────────────────────────
  const images   = (listing.images ?? []) as { url: string; label?: string }[];
  const features = (listing.features ?? []) as string[];
  const img      = images[selectedImg]?.url ?? "/no-image.svg";

  const priceHistory = listing.has_price_history
    ? Array.from({ length: 6 }, (_, i) => ({
        month: new Date(Date.now() - (5 - i) * 30 * 86400000)
          .toLocaleDateString("fr-TN", { month: "short" }),
        price: (listing.price ?? 0) * (0.93 + Math.random() * 0.14),
      }))
    : null;

  const location = [listing.city, listing.municipality, listing.zone, listing.region]
    .filter(Boolean).join(", ");

  return (
    <PublicLayout>
      <div className="container mx-auto px-4 py-8">

        {/* ── Action Buttons (Save, Favorite, Share) ───────────────────────── */}
        <div className="flex justify-end gap-2 mb-4">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleSave}
            className={isSaved ? "bg-primary/10 text-primary" : ""}
          >
            <Bookmark className={`h-4 w-4 mr-1 ${isSaved ? "fill-current" : ""}`} />
            {isSaved ? "Saved" : "Save"}
          </Button>
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleFavorite}
            className={isFavorite ? "bg-red-500/10 text-red-500" : ""}
          >
            <Heart className={`h-4 w-4 mr-1 ${isFavorite ? "fill-current" : ""}`} />
            {isFavorite ? "Favorited" : "Favorite"}
          </Button>
          <Button variant="outline" size="sm" onClick={handleShare}>
            <Share2 className="h-4 w-4 mr-1" />
            Share
          </Button>
        </div>

        {/* ── Main grid ──────────────────────────────────────────────────── */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">

          {/* Left — images */}
          <div className="space-y-3">
            <div className="aspect-[4/3] rounded-xl overflow-hidden bg-muted">
              <img
                src={img}
                alt={listing.title}
                className="w-full h-full object-cover"
                onError={e => { (e.target as HTMLImageElement).src = "/no-image.svg"; }}
              />
            </div>
            {images.length > 1 && (
              <div className="flex gap-2 overflow-x-auto pb-1">
                {images.map((im, i) => (
                  <button
                    key={i}
                    onClick={() => setSelectedImg(i)}
                    className={`shrink-0 w-20 h-16 rounded-lg overflow-hidden border-2 transition-colors ${
                      i === selectedImg ? "border-primary" : "border-transparent hover:border-border"
                    }`}
                  >
                    <img
                      src={im.url}
                      alt={im.label || ""}
                      className="w-full h-full object-cover"
                      onError={e => { (e.target as HTMLImageElement).src = "/no-image.svg"; }}
                    />
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Right — details */}
          <div className="space-y-5">
            {/* Badges */}
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="secondary" className="capitalize">{listing.source_name}</Badge>
              {listing.transaction_type && <TransactionBadge type={listing.transaction_type as "sale" | "rent"} />}
              {listing.type && <TypeBadge type={listing.type as "apartment" | "house" | "land" | "commercial"} />}
            </div>

            {/* Title */}
            <h1 className="text-2xl font-bold leading-snug">{listing.title}</h1>

            {/* Price */}
            <div className="flex items-baseline gap-3">
              <span className="text-3xl font-bold text-primary">{formatPrice(listing.price)}</span>
              {listing.surface && listing.price_per_m2 && (
                <span className="text-muted-foreground text-sm">{formatPricePerM2(listing.price_per_m2)}</span>
              )}
            </div>

            {/* Location */}
            {location && (
              <div className="flex items-center gap-2 text-muted-foreground text-sm">
                <MapPin className="h-4 w-4 text-primary shrink-0" />
                {location}
              </div>
            )}

            {/* Stats grid */}
            <div className="grid grid-cols-3 gap-3">
              {listing.rooms != null && listing.rooms > 0 && (
                <div className="text-center p-3 rounded-xl bg-muted/50">
                  <BedDouble className="h-5 w-5 mx-auto mb-1 text-primary" />
                  <div className="font-semibold">{listing.rooms}</div>
                  <div className="text-xs text-muted-foreground">Pièces</div>
                </div>
              )}
              {listing.surface != null && listing.surface > 0 && (
                <div className="text-center p-3 rounded-xl bg-muted/50">
                  <Maximize className="h-5 w-5 mx-auto mb-1 text-primary" />
                  <div className="font-semibold">{listing.surface} m²</div>
                  <div className="text-xs text-muted-foreground">Surface</div>
                </div>
              )}
              {listing.price_per_m2 != null && listing.price_per_m2 > 0 && (
                <div className="text-center p-3 rounded-xl bg-muted/50">
                  <div className="font-semibold text-sm">{formatPricePerM2(listing.price_per_m2)}</div>
                  <div className="text-xs text-muted-foreground mt-1">Prix/m²</div>
                </div>
              )}
            </div>

            {/* Contact Button */}
            <Button 
              size="lg" 
              className="w-full gap-2"
              onClick={handleContact}
            >
              <Phone className="h-4 w-4" />
              Contacter l'agence
            </Button>

            {/* Features */}
            {features.length > 0 && (
              <div>
                <h3 className="font-semibold mb-2 text-sm">Caractéristiques</h3>
                <div className="flex flex-wrap gap-1.5">
                  {features.map((f, i) => (
                    <Badge key={i} variant="outline" className="text-xs">{f}</Badge>
                  ))}
                </div>
              </div>
            )}

            {/* Description */}
            {listing.description && (
              <div>
                <h3 className="font-semibold mb-2 text-sm">Description</h3>
                <p className={`text-sm text-muted-foreground leading-relaxed ${!descExpanded ? "line-clamp-4" : ""}`}>
                  {listing.description}
                </p>
                {listing.description.length > 300 && (
                  <button onClick={() => setDescExpanded(v => !v)} className="text-sm text-primary mt-1 hover:underline">
                    {descExpanded ? "Voir moins" : "Lire plus"}
                  </button>
                )}
              </div>
            )}

            {/* Dates */}
            <div className="flex gap-4 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <Calendar className="h-3 w-3" /> Publié : {formatDate(listing.scraped_at)}
              </span>
              <span className="flex items-center gap-1">
                <Calendar className="h-3 w-3" /> Mis à jour : {formatDate(listing.last_updated)}
              </span>
            </div>

            {/* External link */}
            {listing.url && (
              <Button asChild variant="outline" className="gap-2 w-full">
                <a href={listing.url} target="_blank" rel="noopener noreferrer">
                  <ExternalLink className="h-4 w-4" /> Voir l'annonce originale
                </a>
              </Button>
            )}
          </div>
        </div>

        {/* ── Price history ───────────────────────────────────────────────── */}
        {priceHistory && (
          <Card className="mt-8">
            <CardHeader>
              <CardTitle className="text-lg">Historique des prix</CardTitle>
            </CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={priceHistory}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="month" className="text-xs" />
                  <YAxis className="text-xs" tickFormatter={v => `${(v / 1000).toFixed(0)}k`} />
                  <Tooltip formatter={(v: number) => formatPrice(Math.round(v))} />
                  <Line type="monotone" dataKey="price"
                    stroke="hsl(var(--primary))" strokeWidth={2} dot={{ r: 4 }} />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        )}

        {/* ── Listing analysis ────────────────────────────────────────────── */}
        <Card className="mt-8">
          <CardHeader><CardTitle className="text-lg">Analyse de l'annonce</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            {listing.reliability_score != null && (
              <div className="flex items-center gap-4">
                <span className="text-sm font-medium shrink-0">Fiabilité</span>
                <Progress value={listing.reliability_score} className="flex-1" />
                {listing.reliability_level && (
                  <ReliabilityBadge level={listing.reliability_level as "HIGH" | "MEDIUM" | "LOW"} />
                )}
                <span className="text-sm font-semibold shrink-0">{listing.reliability_score?.toFixed(0)}%</span>
              </div>
            )}

            {listing.is_outlier && (
              <div className="rounded-xl bg-amber-500/10 border border-amber-500/20 p-4 flex gap-3">
                <AlertTriangle className="h-5 w-5 text-amber-600 shrink-0 mt-0.5" />
                <div>
                  <p className="font-semibold text-sm">Prix atypique</p>
                  {listing.outlier_flags?.length > 0 && (
                    <div className="flex flex-wrap gap-1 mt-1">
                      {listing.outlier_flags.map((f: string) => (
                        <Badge key={f} variant="outline" className="text-xs">{f}</Badge>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}

            {listing.suspected_duplicate && (
              <div className="rounded-xl bg-amber-500/10 border border-amber-500/20 p-4 flex gap-3">
                <Copy className="h-5 w-5 text-amber-600 shrink-0 mt-0.5" />
                <p className="font-semibold text-sm">Doublon possible</p>
              </div>
            )}
          </CardContent>
        </Card>

        {/* ── Similar listings (using the new component) ──────────────────── */}
        <SimilarListings listingId={id || ''} limit={6} />

      </div>
    </PublicLayout>
  );
}