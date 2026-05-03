// components/SimilarListings.tsx
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Heart, Eye, MapPin } from "lucide-react";
import { Link } from "react-router-dom";
import { useSimilarListings } from "@/hooks/useListings";

interface SimilarListingsProps {
  listingId: string;
  limit?: number;
}

export default function SimilarListings({ listingId, limit = 6 }: SimilarListingsProps) {
  const { data, loading, error } = useSimilarListings(listingId, limit);

  // Handle loading state
  if (loading) {
    return (
      <div className="mt-8">
        <h2 className="text-xl font-bold mb-4">Similar Properties</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[...Array(3)].map((_, i) => (
            <Card key={i}>
              <Skeleton className="h-48 w-full rounded-t-lg" />
              <CardContent className="p-3 space-y-2">
                <Skeleton className="h-4 w-3/4" />
                <Skeleton className="h-4 w-1/2" />
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  // Handle error or no data - check if data exists and is an array
  if (error || !data || !Array.isArray(data) || data.length === 0) {
    return null;
  }

  // Helper function to safely format price
  const formatPrice = (price: number | null | undefined): string => {
    if (!price || isNaN(price)) return "Price on request";
    return price.toLocaleString("fr-TN") + " TND";
  };

  // Helper function to safely get image URL
  const getImageUrl = (images: any[] | null | undefined): string => {
    if (!images || !Array.isArray(images) || images.length === 0) return "/no-image.svg";
    const firstImage = images[0];
    if (typeof firstImage === 'string') return firstImage;
    if (firstImage && typeof firstImage === 'object' && firstImage.url) return firstImage.url;
    return "/no-image.svg";
  };

  // Safely get listing title
  const getTitle = (listing: any): string => {
    return listing?.title || "Property";
  };

  // Safely get city
  const getCity = (listing: any): string => {
    return listing?.city || "Location not specified";
  };

  // Safely get surface
  const getSurface = (listing: any): number | null => {
    return listing?.surface || null;
  };

  // Safely get views count
  const getViewsCount = (listing: any): number => {
    const count = listing?.views_count;
    return typeof count === 'number' ? count : 0;
  };

  // Safely get likes count
  const getLikesCount = (listing: any): number => {
    const count = listing?.likes_count;
    return typeof count === 'number' ? count : 0;
  };

  // Safely get transaction type
  const getTransactionType = (listing: any): string => {
    return listing?.transaction_type || "sale";
  };

  return (
    <div className="mt-8">
      <h2 className="text-xl font-bold mb-4">Similar Properties</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {data.map((listing: any, index: number) => (
          <Link key={listing?.id || index} to={`/listing/${listing?.id || ''}`}>
            <Card className="overflow-hidden hover:shadow-lg transition-shadow cursor-pointer h-full">
              <div className="relative h-48">
                <img
                  src={getImageUrl(listing?.images)}
                  alt={getTitle(listing)}
                  className="w-full h-full object-cover"
                  onError={(e) => {
                    (e.target as HTMLImageElement).src = "/no-image.svg";
                  }}
                />
                <Badge className="absolute top-2 right-2 bg-black/50 text-white">
                  {getTransactionType(listing) === "sale" ? "For Sale" : "For Rent"}
                </Badge>
              </div>
              <CardContent className="p-3 space-y-2">
                <h3 className="font-semibold text-sm line-clamp-2">
                  {getTitle(listing)}
                </h3>
                <p className="text-lg font-bold text-primary">
                  {formatPrice(listing?.price)}
                </p>
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <MapPin className="h-3 w-3" />
                  <span className="truncate">{getCity(listing)}</span>
                  {getSurface(listing) && (
                    <>
                      <span>•</span>
                      <span>{getSurface(listing)} m²</span>
                    </>
                  )}
                </div>
                <div className="flex items-center gap-3 text-xs text-muted-foreground">
                  <div className="flex items-center gap-1">
                    <Eye className="h-3 w-3" />
                    <span>{getViewsCount(listing)}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <Heart className="h-3 w-3" />
                    <span>{getLikesCount(listing)}</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}