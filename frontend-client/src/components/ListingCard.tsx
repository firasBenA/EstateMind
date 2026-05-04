/**
 * frontend-client/src/components/ListingCard.tsx
 *
 * Uses the Listing type from api.ts (real PostgreSQL data).
 */
import { useState } from "react";
import { Link }        from "react-router-dom";
import { Card, CardContent } from "@/components/ui/card";
import { Badge }       from "@/components/ui/badge";
import { type Listing } from "@/lib/api";
import { ReliabilityBadge, TypeBadge, TransactionBadge } from "./Badges";
import { BedDouble, Maximize } from "lucide-react";
import { ReliabilityScoreBadge } from "@/components/ReliabilityScoreBadge";
import { ForecastingButton } from "./ForecastingButton";

function formatPrice(price: number | null): string {
  if (price == null) return "Prix sur demande";
  return price.toLocaleString("fr-TN") + " TND";
}

function formatPricePerM2(v: number | null): string {
  if (v == null || v <= 0) return "";
  return v.toLocaleString("fr-TN") + " TND/m²";
}

export function ListingCard({ listing }: { listing: Listing }) {
  const [imgIdx, setImgIdx] = useState(0);

  // images come from API as [{url, label}] — normalise defensively
  const images = ((listing.images ?? []) as (string | { url: string; label?: string })[])
    .map(i => typeof i === "string" ? { url: i, label: "photo" } : i)
    .filter(i => i?.url);
  const img = images[imgIdx]?.url ?? "/no-image.svg";

  return (
    <Link to={`/listing/${listing.id}`} className="group">
      <Card className="overflow-hidden transition-all hover:shadow-lg hover:-translate-y-1 h-full">

        {/* Image */}
        <div className="relative aspect-[4/3] bg-muted overflow-hidden">
          <img
            src={img}
            alt={listing.title}
            className="w-full h-full object-cover transition-transform group-hover:scale-105"
            loading="lazy"
            onError={e => { (e.target as HTMLImageElement).src = "/no-image.svg"; }}
          />

          {/* Image dots */}
          {images.length > 1 && (
            <div className="absolute bottom-2 right-2 flex gap-1">
              {images.slice(0, 4).map((_, i) => (
                <button
                  key={i}
                  className={`w-2 h-2 rounded-full transition-colors ${i === imgIdx ? "bg-white" : "bg-white/50"}`}
                  onClick={e => { e.preventDefault(); setImgIdx(i); }}
                />
              ))}
            </div>
          )}

          {/* Fraud banner */}
          {/* {listing.fraud_flag && (
            <div className="absolute top-0 inset-x-0 bg-destructive/90 text-destructive-foreground text-xs font-medium py-1 text-center">
              Annonce suspecte
            </div>
          )} */}

          {/* Transaction badge */}
          <div className="absolute top-2 left-2 flex gap-1">
            {listing.transaction_type && (
              <TransactionBadge type={listing.transaction_type} />
            )}
          </div>
        </div>

        {/* Content */}
        <CardContent className="p-4 space-y-2">
          <div className="flex items-start justify-between gap-2">
            <h3 className="font-semibold text-sm line-clamp-2 group-hover:text-primary transition-colors">
              {listing.title}
            </h3>
            <ReliabilityScoreBadge listingId={listing.id} size="sm" showLabel={false} />
          </div>

          <p className="text-xs text-muted-foreground">
            {listing.city}{listing.zone ? ` • ${listing.zone}` : ""}
          </p>

          <div className="flex items-baseline justify-between">
            <span className="text-lg font-bold text-primary">
              {formatPrice(listing.price)}
            </span>
            {listing.surface && listing.price_per_m2 && listing.price_per_m2 > 0 && (
              <span className="text-xs text-muted-foreground">
                {formatPricePerM2(listing.price_per_m2)}
              </span>
            )}
          </div>

          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            {listing.type && <TypeBadge type={listing.type} />}
            {listing.rooms != null && listing.rooms > 0 && (
              <span className="flex items-center gap-1">
                <BedDouble className="h-3 w-3" /> {listing.rooms}
              </span>
            )}
            {listing.surface != null && listing.surface > 0 && (
              <span className="flex items-center gap-1">
                <Maximize className="h-3 w-3" /> {listing.surface} m²
              </span>
            )}
          </div>

          {(listing.suspected_duplicate || listing.is_outlier) && (
            <div className="flex gap-1 flex-wrap">
              {listing.suspected_duplicate && (
                <Badge variant="outline" className="text-[10px] border-amber-300/50 text-amber-600">
                  Doublon possible
                </Badge>
              )}
              {listing.is_outlier && (
                <Badge variant="outline" className="text-[10px] border-amber-300/50 text-amber-600">
                  Prix atypique
                </Badge>
              )}
            </div>
          )}

          {/* Forecasting — tout sauf location */}
          {listing.transaction_type !== "rent" && (
            <div className="pt-1">
              <ForecastingButton listing={listing} variant="card" />
            </div>
          )}
        </CardContent>
      </Card>
    </Link>
  );
}