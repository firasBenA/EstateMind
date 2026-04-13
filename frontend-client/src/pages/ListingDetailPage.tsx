import { useParams, Link } from "react-router-dom";
import { PublicLayout } from "@/components/PublicLayout";
import { ListingCard } from "@/components/ListingCard";
import { formatPrice, formatPricePerM2, formatDate } from "@/lib/mock-data";
import { ReliabilityBadge, TypeBadge, TransactionBadge } from "@/components/Badges";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { MapPin, BedDouble, Maximize, ExternalLink, Calendar, AlertTriangle, ShieldAlert, Copy } from "lucide-react";
import { useState, useEffect } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

export default function ListingDetailPage() {
  const { id } = useParams();
  
  // ALL hooks at the top
  const [listing, setListing] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedImg, setSelectedImg] = useState(0);
  const [descExpanded, setDescExpanded] = useState(false);
  const [similar, setSimilar] = useState([]);

  // Fetch main listing
  useEffect(() => {
    fetch(`http://localhost:8000/api/listings/${id}/`)
      .then(res => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then(data => {
        setListing(data);
        setLoading(false);
      })
      .catch(err => {
        console.error("Fetch error:", err);
        setError(err.message);
        setLoading(false);
      });
  }, [id]);

  // Fetch similar listings
  useEffect(() => {
    if (listing) {
      fetch(`http://localhost:8000/api/listings/?type=${listing.type}&city=${listing.city}&page_size=4`)
        .then(res => res.json())
        .then(data => {
          const similarListings = (data.results || []).filter(l => l.id !== listing.id);
          setSimilar(similarListings);
        })
        .catch(err => console.error("Error fetching similar:", err));
    }
  }, [listing]);

  if (loading) {
    return (
      <PublicLayout>
        <div className="container mx-auto px-4 py-20 text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto"></div>
          <p className="mt-4 text-muted-foreground">Loading listing...</p>
        </div>
      </PublicLayout>
    );
  }

  if (error || !listing) {
    return (
      <PublicLayout>
        <div className="container mx-auto px-4 py-20 text-center">
          <h1 className="text-2xl font-bold mb-4">Listing not found</h1>
          <Button asChild><Link to="/search">Back to Search</Link></Button>
        </div>
      </PublicLayout>
    );
  }

  // Price history mock data (if listing has price history)
  const priceHistory = listing.has_price_history
    ? Array.from({ length: 6 }, (_, i) => ({
        month: new Date(Date.now() - (5 - i) * 30 * 86400000).toLocaleDateString("en", { month: "short" }),
        price: listing.price + (Math.random() - 0.5) * listing.price * 0.1,
      }))
    : null;

  return (
    <PublicLayout>
      <div className="container mx-auto px-4 py-8">
        {/* Two column layout */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          
          {/* LEFT COLUMN: Images */}
          <div className="space-y-3">
            {/* Main Image */}
            <div className="aspect-[4/3] rounded-lg overflow-hidden bg-muted">
              <img 
                src={listing.images?.[selectedImg]?.url || listing.images?.[0]?.url} 
                alt={listing.title} 
                className="w-full h-full object-cover" 
              />
            </div>
            
            {/* Thumbnails */}
            {listing.images && listing.images.length > 0 && (
              <>
                <div className="flex gap-2 overflow-x-auto">
                  {listing.images.map((img, i) => (
                    <button
                      key={i}
                      onClick={() => setSelectedImg(i)}
                      className={`shrink-0 w-20 h-16 rounded-md overflow-hidden border-2 transition-colors ${
                        i === selectedImg ? "border-primary" : "border-transparent"
                      }`}
                    >
                      <img src={img.url} alt={img.label} className="w-full h-full object-cover" />
                    </button>
                  ))}
                </div>
                <div className="flex flex-wrap gap-1">
                  {listing.images.map((img, i) => (
                    <Badge key={i} variant="outline" className="text-[10px]">
                      {img.label || "Photo"}
                    </Badge>
                  ))}
                </div>
              </>
            )}
          </div>

          {/* RIGHT COLUMN: Details */}
          <div className="space-y-6">
            {/* Badges */}
            <div>
              <div className="flex items-center gap-2 mb-2 flex-wrap">
                <Badge variant="secondary" className="capitalize">{listing.source_name}</Badge>
                <TransactionBadge type={listing.transaction_type} />
                <TypeBadge type={listing.type} />
              </div>
              <h1 className="text-2xl font-bold">{listing.title}</h1>
            </div>

            {/* Price */}
            <div className="flex items-baseline gap-3">
              <span className="text-3xl font-bold text-primary">{formatPrice(listing.price)}</span>
              {listing.surface > 0 && listing.price_per_m2 && (
                <span className="text-muted-foreground">{formatPricePerM2(listing.price_per_m2)}</span>
              )}
            </div>

            {/* Location */}
            <div className="flex items-center gap-2 text-muted-foreground">
              <MapPin className="h-4 w-4 text-primary" />
              <span>{[listing.city, listing.municipality, listing.zone, listing.region].filter(Boolean).join(", ")}</span>
            </div>

            {/* Key Metrics */}
            <div className="grid grid-cols-2 gap-4">
              {listing.rooms > 0 && (
                <div className="text-center p-3 rounded-lg bg-muted/50">
                  <BedDouble className="h-5 w-5 mx-auto mb-1 text-primary" />
                  <div className="font-semibold">{listing.rooms}</div>
                  <div className="text-xs text-muted-foreground">Rooms</div>
                </div>
              )}
              {listing.surface > 0 && (
                <div className="text-center p-3 rounded-lg bg-muted/50">
                  <Maximize className="h-5 w-5 mx-auto mb-1 text-primary" />
                  <div className="font-semibold">{listing.surface} m²</div>
                  <div className="text-xs text-muted-foreground">Surface</div>
                </div>
              )}
            </div>

            {/* Features */}
            {listing.features && listing.features.length > 0 && (
              <div>
                <h3 className="font-semibold mb-2">Features</h3>
                <div className="flex flex-wrap gap-2">
                  {listing.features.map(f => (
                    <Badge key={f} variant="outline">{f}</Badge>
                  ))}
                </div>
              </div>
            )}

            {/* Description */}
            {listing.description && (
              <div>
                <h3 className="font-semibold mb-2">Description</h3>
                <p className={`text-sm text-muted-foreground ${!descExpanded ? "line-clamp-4" : ""}`}>
                  {listing.description}
                </p>
                {listing.description.length > 300 && (
                  <button 
                    onClick={() => setDescExpanded(!descExpanded)} 
                    className="text-sm text-primary mt-1"
                  >
                    {descExpanded ? "Show less" : "Read more"}
                  </button>
                )}
              </div>
            )}

            {/* Dates */}
            <div className="flex gap-4 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <Calendar className="h-3 w-3" /> Scraped: {formatDate(listing.scraped_at)}
              </span>
              <span className="flex items-center gap-1">
                <Calendar className="h-3 w-3" /> Updated: {formatDate(listing.last_updated)}
              </span>
            </div>

            {/* Original Listing Button */}
            {listing.url && (
              <Button asChild variant="outline" className="gap-2">
                <a href={listing.url} target="_blank" rel="noopener noreferrer">
                  <ExternalLink className="h-4 w-4" /> View Original Listing
                </a>
              </Button>
            )}
          </div>
        </div>

        {/* Price History Chart */}
        {priceHistory && (
          <Card className="mt-8">
            <CardHeader><CardTitle className="text-lg">Price History</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={priceHistory}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="month" className="text-xs" />
                  <YAxis className="text-xs" tickFormatter={v => `${(v / 1000).toFixed(0)}k`} />
                  <Tooltip formatter={(v: number) => formatPrice(Math.round(v))} />
                  <Line type="monotone" dataKey="price" stroke="hsl(var(--primary))" strokeWidth={2} dot={{ r: 4 }} />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        )}

        {/* Fraud & Analysis Section */}
        <Card className="mt-8">
          <CardHeader><CardTitle className="text-lg">Listing Analysis</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            {/* Reliability Score */}
            {listing.reliability_score && (
              <div className="flex items-center gap-4">
                <span className="text-sm font-medium">Reliability</span>
                <Progress value={listing.reliability_score} className="flex-1" />
                <ReliabilityBadge level={listing.reliability_level} />
                <span className="text-sm font-semibold">{listing.reliability_score}%</span>
              </div>
            )}

            {/* Fraud Alert */}
            {listing.fraud_flag && (
              <div className="rounded-lg bg-destructive/10 border border-destructive/20 p-4 flex gap-3">
                <ShieldAlert className="h-5 w-5 text-destructive shrink-0 mt-0.5" />
                <div>
                  <p className="font-semibold text-destructive text-sm">Fraud Detected</p>
                  <p className="text-sm text-muted-foreground">{listing.fraud_reason}</p>
                </div>
              </div>
            )}

            {/* Outlier Alert */}
            {listing.is_outlier && (
              <div className="rounded-lg bg-warning/10 border border-warning/20 p-4 flex gap-3">
                <AlertTriangle className="h-5 w-5 text-warning shrink-0 mt-0.5" />
                <div>
                  <p className="font-semibold text-sm">Price Outlier</p>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {listing.outlier_flags?.map(f => (
                      <Badge key={f} variant="outline" className="text-xs">{f}</Badge>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Duplicate Alert */}
            {listing.suspected_duplicate && (
              <div className="rounded-lg bg-warning/10 border border-warning/20 p-4 flex gap-3">
                <Copy className="h-5 w-5 text-warning shrink-0 mt-0.5" />
                <p className="font-semibold text-sm">Suspected Duplicate</p>
              </div>
            )}

            {/* Room/Image Ratio */}
            {listing.images_count && listing.rooms && (
              <div className="text-sm text-muted-foreground">
                Room/Image Ratio: {listing.images_count} images for {listing.rooms} declared rooms
              </div>
            )}
          </CardContent>
        </Card>

        {/* Similar Listings */}
        {similar.length > 0 && (
          <section className="mt-12">
            <h2 className="text-xl font-bold mb-6">Similar Listings</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              {similar.map(l => <ListingCard key={l.id} listing={l} />)}
            </div>
          </section>
        )}
      </div>
    </PublicLayout>
  );
}