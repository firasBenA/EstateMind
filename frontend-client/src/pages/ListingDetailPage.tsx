/**
 * frontend-client/src/pages/ListingDetailPage.tsx
 */
import { useParams, Link } from "react-router-dom";
import { PublicLayout } from "@/components/PublicLayout";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { ReliabilityScoreBadge } from "@/components/ReliabilityScoreBadge";
import { DescriptionAnalysis } from "@/components/DescriptionAnalysis";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { useListing, useSimilarListings } from "@/hooks/useListings";
import { useBehaviorTracker } from "@/hooks/useBehaviorTracker.ts";
import { useAuth } from "@/lib/auth-context";
import {
  TypeBadge,
  TransactionBadge,
} from "@/components/Badges";
import SimilarListings from "@/components/SimilarListings";
import {
  MapPin,
  BedDouble,
  Maximize,
  ExternalLink,
  Calendar,
  AlertTriangle,
  Copy,
  Heart,
  Share2,
  Bookmark,
  Loader2,
  MessageCircle,
  TrendingUp,
  TrendingDown,
} from "lucide-react";
import { useState, useEffect, useCallback } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
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
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

const isUserSubmission = (sourceName: string | undefined): boolean =>
  sourceName === "user_submission" || sourceName === "user";

const normalizeMismatchTypes = (mismatchTypes: any): string[] => {
  if (!mismatchTypes) return [];
  if (Array.isArray(mismatchTypes)) return mismatchTypes;
  if (typeof mismatchTypes === "string") {
    try {
      const parsed = JSON.parse(mismatchTypes);
      return Array.isArray(parsed) ? parsed : [mismatchTypes];
    } catch {
      return mismatchTypes.split(",").map((s: string) => s.trim());
    }
  }
  return [];
};

// ── Contact Modal ─────────────────────────────────────────────────────────────

function ContactModal({
  listing,
  onClose,
  onSend,
}: {
  listing: any;
  onClose: () => void;
  onSend: (message: string) => void;
}) {
  const [message, setMessage] = useState("");
  const [name, setName] = useState("");
  const [phone, setPhone] = useState("");
  const [sending, setSending] = useState(false);

  const handleSend = async () => {
    if (!message.trim()) return;
    setSending(true);
    await onSend(message);
    setSending(false);
    onClose();
  };

  return (
    <Dialog open onOpenChange={onClose}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Contacter {isUserSubmission(listing.source_name) ? "le propriétaire" : "l'agence"}</DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="Votre nom" />
          <Input value={phone} onChange={(e) => setPhone(e.target.value)} placeholder="+216 XX XXX XXX" />
          <Textarea
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            placeholder={`Je suis intéressé par: ${listing.title}`}
            rows={4}
          />
          <Button onClick={handleSend} disabled={sending || !message.trim()} className="w-full">
            {sending && <Loader2 className="h-4 w-4 animate-spin mr-2" />}
            {sending ? "Envoi..." : "Envoyer"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function ImageSkeleton() {
  return (
    <div className="space-y-3">
      <Skeleton className="aspect-[4/3] w-full rounded-lg" />
      <div className="flex gap-2">
        {[0, 1, 2].map((i) => <Skeleton key={i} className="w-20 h-16 rounded-md" />)}
      </div>
    </div>
  );
}

// ── Main Component ────────────────────────────────────────────────────────────

export default function ListingDetailPage() {
  const { id } = useParams<{ id: string }>();

  const { isAuthenticated, loading: authLoading } = useAuth();
  const { track } = useBehaviorTracker();

  const [selectedImg, setSelectedImg] = useState(0);
  const [descExpanded, setDescExpanded] = useState(false);
  const [viewTracked, setViewTracked] = useState(false);
  const [isSaved, setIsSaved] = useState(false);
  const [isFavorite, setIsFavorite] = useState(false);
  const [showContactModal, setShowContactModal] = useState(false);
  const [fraudScore, setFraudScore] = useState<any>(null);
  const [analysisTab, setAnalysisTab] = useState<"global" | "text">("global");

  const { listing, loading, error } = useListing(id ?? null);
  const { data: simData } = useSimilarListings(id ?? null, 6);

  // Track view after 2 seconds
  useEffect(() => {
    if (!loading && listing && !viewTracked && isAuthenticated) {
      const timer = setTimeout(async () => {
        try {
          await fetch(`/api/listings/${listing.id}/view/`, {
            method: "POST",
            credentials: "include",
          });
          track("view", listing.id, {
            duration: 30,
            referrer: document.referrer || "direct",
          });
          setViewTracked(true);
        } catch (err) {
          console.error("Error tracking view:", err);
        }
      }, 2000);
      return () => clearTimeout(timer);
    }
  }, [loading, listing, viewTracked, isAuthenticated, track]);

  // Fetch fraud score
  useEffect(() => {
    const fetchFraudScore = async () => {
      if (!listing?.id) return;
      try {
        const response = await fetch(`/api/listing/fraud-score/${listing.id}/`, {
          credentials: "include",
        });
        const data = await response.json();
        if (data.success) {
          setFraudScore(data.data);
        }
      } catch (error) {
        console.error("Error fetching fraud score:", error);
      }
    };
    fetchFraudScore();
  }, [listing?.id]);

  const handleSave = useCallback(async () => {
    if (!listing) return;
    setIsSaved((v) => !v);
    if (isAuthenticated && !isSaved) {
      await track("save", listing.id, { referrer: "detail_page" });
    }
  }, [listing, isAuthenticated, isSaved, track]);

  const handleFavorite = useCallback(async () => {
    if (!listing) return;
    setIsFavorite((v) => !v);
    if (isAuthenticated && !isFavorite) {
      await track("favorite", listing.id, { referrer: "detail_page" });
      try {
        const res = await fetch(`/api/listings/${listing.id}/like/`, {
          method: "POST",
          credentials: "include",
        });
        if (!res.ok) {
          const err = await res.json();
          if (err.error) {
            alert(err.error);
            setIsFavorite(false);
          }
        }
      } catch {
        setIsFavorite(false);
      }
    }
  }, [listing, isAuthenticated, isFavorite, track]);

  const handleContact = useCallback(async () => {
    if (!listing) return;
    if (authLoading) return;
    if (!isAuthenticated) {
      sessionStorage.setItem("redirectAfterLogin", window.location.pathname);
      window.location.href = "/login";
      return;
    }
    try {
      await track("contact", listing.id, { referrer: "detail_page" });
      const res = await fetch(`/api/chat/conversation/${encodeURIComponent(listing.id)}/`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
      });
      const data = await res.json();
      if (res.ok) {
        window.location.href = `/user/messages?conversation=${data.conversation_id}`;
      } else {
        alert(data.error || "Impossible de démarrer la conversation");
      }
    } catch (err) {
      console.error("Error starting chat:", err);
      alert("Erreur lors de la création de la conversation");
    }
  }, [listing, isAuthenticated, authLoading, track]);

  const handleSendMessage = useCallback(async (message: string) => {
    if (!listing) return;
    try {
      const res = await fetch("/api/contact/", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          listing_id: listing.id,
          message,
          recipient_type: isUserSubmission(listing.source_name) ? "owner" : "agency",
        }),
      });
      if (res.ok) {
        alert("Message envoyé avec succès !");
      } else {
        const err = await res.json();
        alert(err.error || "Échec de l'envoi");
      }
    } catch {
      alert("Erreur lors de l'envoi");
    }
  }, [listing]);

  const handleShare = useCallback(() => {
    if (navigator.share && listing) {
      navigator.share({ title: listing.title, url: window.location.href });
    } else {
      navigator.clipboard.writeText(window.location.href);
      alert("Lien copié !");
    }
  }, [listing]);

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

  if (error || !listing) {
    return (
      <PublicLayout>
        <div className="container mx-auto px-4 py-20 text-center">
          <h1 className="text-2xl font-bold mb-4">Annonce introuvable</h1>
          <p className="text-muted-foreground mb-6">
            {error ?? "Cette annonce n'existe pas ou a été supprimée."}
          </p>
          <Button asChild>
            <Link to="/search">Retour à la recherche</Link>
          </Button>
        </div>
      </PublicLayout>
    );
  }

  const images = (listing.images ?? []) as { url: string; label?: string }[];
  const features = (listing.features ?? []) as string[];
  const img = images[selectedImg]?.url ?? "/no-image.svg";

  const priceHistory = listing.has_price_history
    ? Array.from({ length: 6 }, (_, i) => ({
        month: new Date(Date.now() - (5 - i) * 30 * 86400000).toLocaleDateString("fr-TN", { month: "short" }),
        price: (listing.price ?? 0) * (0.93 + Math.random() * 0.14),
      }))
    : null;

  const location = [listing.city, listing.municipality, listing.zone, listing.region].filter(Boolean).join(", ");
  const mismatchTypes = fraudScore ? normalizeMismatchTypes(fraudScore.mismatch_types) : [];
  const scorePercent = fraudScore ? Math.round(fraudScore.score * 100) : 0;

  const getScoreColor = (score: number) => {
    if (score < 30) return "text-red-600";
    if (score < 60) return "text-orange-500";
    return "text-green-600";
  };

  const getScoreBg = (score: number) => {
    if (score < 30) return "bg-red-50";
    if (score < 60) return "bg-orange-50";
    return "bg-green-50";
  };

  return (
    <PublicLayout>
      <div className="container mx-auto px-4 py-8 max-w-7xl">
        {/* Action buttons */}
        <div className="flex justify-end gap-2 mb-4">
          <Button variant="ghost" size="sm" onClick={handleSave} className={isSaved ? "text-primary" : ""}>
            <Bookmark className={`h-4 w-4 mr-1 ${isSaved ? "fill-current" : ""}`} />
            {isSaved ? "Sauvegardé" : "Sauvegarder"}
          </Button>
          <Button variant="ghost" size="sm" onClick={handleFavorite} className={isFavorite ? "text-red-500" : ""}>
            <Heart className={`h-4 w-4 mr-1 ${isFavorite ? "fill-current" : ""}`} />
            {isFavorite ? "Favori" : "Ajouter aux favoris"}
          </Button>
          <Button variant="ghost" size="sm" onClick={handleShare}>
            <Share2 className="h-4 w-4 mr-1" /> Partager
          </Button>
        </div>

        {/* Main grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Images */}
          <div className="space-y-3">
            <div className="aspect-[4/3] rounded-xl overflow-hidden bg-gray-100">
              <img src={img} alt={listing.title} className="w-full h-full object-cover" />
            </div>
            {images.length > 1 && (
              <div className="flex gap-2 overflow-x-auto pb-1">
                {images.map((im, i) => (
                  <button
                    key={i}
                    onClick={() => setSelectedImg(i)}
                    className={`shrink-0 w-20 h-16 rounded-lg overflow-hidden border-2 transition-all ${
                      i === selectedImg ? "border-primary ring-2 ring-primary/20" : "border-transparent"
                    }`}
                  >
                    <img src={im.url} alt={im.label || ""} className="w-full h-full object-cover" />
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Details */}
          <div className="space-y-5">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="secondary" className="capitalize">{listing.source_name}</Badge>
              {listing.transaction_type && <TransactionBadge type={listing.transaction_type as "sale" | "rent"} />}
              {listing.type && <TypeBadge type={listing.type as "apartment" | "house" | "land" | "commercial"} />}
            </div>

            <h1 className="text-2xl font-bold leading-snug">{listing.title}</h1>

            <div className="flex items-baseline gap-3">
              <span className="text-3xl font-bold text-primary">{formatPrice(listing.price)}</span>
              {listing.surface && listing.price_per_m2 && (
                <span className="text-muted-foreground text-sm">{formatPricePerM2(listing.price_per_m2)}</span>
              )}
            </div>

            {location && (
              <div className="flex items-center gap-2 text-muted-foreground text-sm">
                <MapPin className="h-4 w-4 text-primary shrink-0" /> {location}
              </div>
            )}

            {/* Caractéristiques */}
            <div className="grid grid-cols-3 gap-3">
              {listing.rooms != null && listing.rooms > 0 && (
                <div className="text-center p-3 rounded-xl bg-gray-50">
                  <BedDouble className="h-5 w-5 mx-auto mb-1 text-gray-500" />
                  <div className="font-semibold">{listing.rooms}</div>
                  <div className="text-xs text-muted-foreground">Pièces</div>
                </div>
              )}
              {listing.surface != null && listing.surface > 0 && (
                <div className="text-center p-3 rounded-xl bg-gray-50">
                  <Maximize className="h-5 w-5 mx-auto mb-1 text-gray-500" />
                  <div className="font-semibold">{listing.surface} m²</div>
                  <div className="text-xs text-muted-foreground">Surface</div>
                </div>
              )}
              {listing.price_per_m2 != null && listing.price_per_m2 > 0 && (
                <div className="text-center p-3 rounded-xl bg-gray-50">
                  <div className="font-semibold text-sm">{formatPricePerM2(listing.price_per_m2)}</div>
                  <div className="text-xs text-muted-foreground mt-1">Prix/m²</div>
                </div>
              )}
            </div>

            <Button size="lg" className="w-full gap-2" onClick={handleContact} disabled={authLoading}>
              {authLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <MessageCircle className="h-4 w-4" />}
              {authLoading ? "Chargement..." : `Contacter ${isUserSubmission(listing.source_name) ? "le propriétaire" : "l'agence"}`}
            </Button>

            {features.length > 0 && (
              <div>
                <h3 className="font-semibold mb-2 text-sm">Caractéristiques</h3>
                <div className="flex flex-wrap gap-1.5">
                  {features.map((f, i) => <Badge key={i} variant="outline" className="text-xs font-normal">{f}</Badge>)}
                </div>
              </div>
            )}

            {listing.description && (
              <div>
                <h3 className="font-semibold mb-2 text-sm">Description</h3>
                <p className={`text-sm text-muted-foreground leading-relaxed ${!descExpanded ? "line-clamp-4" : ""}`}>
                  {listing.description}
                </p>
                {listing.description.length > 300 && (
                  <button onClick={() => setDescExpanded((v) => !v)} className="text-sm text-primary mt-1 hover:underline">
                    {descExpanded ? "Voir moins" : "Lire plus"}
                  </button>
                )}
              </div>
            )}

            <div className="flex gap-4 text-xs text-muted-foreground">
              <span className="flex items-center gap-1"><Calendar className="h-3 w-3" /> Publié: {formatDate(listing.scraped_at)}</span>
              <span className="flex items-center gap-1"><Calendar className="h-3 w-3" /> Mis à jour: {formatDate(listing.last_updated)}</span>
            </div>

            {listing.url && (
              <Button asChild variant="outline" className="gap-2 w-full">
                <a href={listing.url} target="_blank" rel="noopener noreferrer">
                  <ExternalLink className="h-4 w-4" /> Voir l'annonce originale
                </a>
              </Button>
            )}
          </div>
        </div>

        {/* Price history */}
        {priceHistory && (
          <Card className="mt-8">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-semibold">Historique des prix</CardTitle>
            </CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={priceHistory}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-gray-100" />
                  <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => `${Math.round(v / 1000)}k`} />
                  <Tooltip formatter={(v: number) => formatPrice(Math.round(v))} />
                  <Line type="monotone" dataKey="price" stroke="hsl(var(--primary))" strokeWidth={2} dot={{ r: 3 }} />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        )}

        {/* Analysis Section - Version Professionnelle */}
        <div className="mt-8 space-y-6">
          <div className="border-b pb-3 flex items-center justify-between flex-wrap gap-3">
            <h2 className="text-xl font-semibold">Analyse de l'annonce</h2>
            <div className="inline-flex rounded-lg border border-gray-200 bg-gray-50 p-0.5 gap-0.5">
              <button
                onClick={() => setAnalysisTab("global")}
                className={`px-3 py-1.5 text-sm font-medium rounded-md transition-all ${
                  analysisTab === "global"
                    ? "bg-white shadow-sm text-gray-900"
                    : "text-gray-500 hover:text-gray-700"
                }`}
              >
                Fiabilité globale
              </button>
              <button
                onClick={() => setAnalysisTab("text")}
                className={`px-3 py-1.5 text-sm font-medium rounded-md transition-all ${
                  analysisTab === "text"
                    ? "bg-white shadow-sm text-gray-900"
                    : "text-gray-500 hover:text-gray-700"
                }`}
              >
                Analyse du texte
              </button>
            </div>
          </div>

          {/* Score de fiabilité */}
          {analysisTab === "global" && fraudScore && (
            <div className={`rounded-lg p-4 ${getScoreBg(scorePercent)} border`}>
              <div className="flex items-start justify-between flex-wrap gap-4">
                <div className="flex-1 min-w-[200px]">
                  <div className="flex items-center gap-2 mb-2">
                    <div className={`text-lg font-bold ${getScoreColor(scorePercent)}`}>
                      {scorePercent}% de fiabilité
                    </div>
                    <ReliabilityScoreBadge listingId={listing.id} size="sm" showLabel={false} />
                  </div>
                  <Progress value={scorePercent} className="h-2" />
                  <p className="text-xs text-gray-500 mt-2">
                    Basé sur {fraudScore.images_analyzed || 0} images analysées
                    {fraudScore.price_deviation !== 0 && (
                      <span className="ml-2">
                        • Écart prix:
                        <span className={fraudScore.price_deviation > 0 ? "text-red-500" : "text-green-500"}>
                          {fraudScore.price_deviation > 0 ? "+" : ""}{fraudScore.price_deviation}%
                        </span>
                      </span>
                    )}
                  </p>
                </div>

                {fraudScore.price_deviation !== 0 && (
                  <div className="flex items-center gap-1 text-sm">
                    {fraudScore.price_deviation > 0 ? (
                      <TrendingUp className="h-4 w-4 text-red-500" />
                    ) : (
                      <TrendingDown className="h-4 w-4 text-green-500" />
                    )}
                    <span className={fraudScore.price_deviation > 0 ? "text-red-500" : "text-green-500"}>
                      {fraudScore.price_deviation > 0 ? "+" : ""}{fraudScore.price_deviation}% vs marché
                    </span>
                  </div>
                )}
              </div>

              {/* Anomalies */}
              {mismatchTypes.length > 0 && (
                <div className="mt-3 pt-3 border-t border-gray-200">
                  <div className="flex flex-wrap gap-1">
                    {mismatchTypes.slice(0, 3).map((type: string, idx: number) => (
                      <Badge key={idx} variant="outline" className="text-xs bg-red-50 text-red-700 border-red-200">
                        {type}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Analyse IA de la description */}
          {analysisTab === "text" && <DescriptionAnalysis listingId={listing.id} />}

          {/* Alertes */}
          <div className="space-y-3">
            {listing.is_outlier && (
              <div className="rounded-lg border border-orange-200 bg-orange-50 p-3 flex gap-3">
                <AlertTriangle className="h-5 w-5 text-orange-600 shrink-0" />
                <div>
                  <p className="font-medium text-sm text-orange-800">Prix atypique détecté</p>
                  {listing.outlier_flags?.length > 0 && (
                    <div className="flex flex-wrap gap-1 mt-1">
                      {listing.outlier_flags.map((f: string) => (
                        <Badge key={f} variant="outline" className="text-xs bg-orange-100 text-orange-700 border-orange-200">
                          {f}
                        </Badge>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}

            {listing.suspected_duplicate && (
              <div className="rounded-lg border border-orange-200 bg-orange-50 p-3 flex gap-3">
                <Copy className="h-5 w-5 text-orange-600 shrink-0" />
                <p className="font-medium text-sm text-orange-800">Doublon possible détecté</p>
              </div>
            )}
          </div>
        </div>

        <SimilarListings listingId={id || ""} limit={6} />

        {showContactModal && (
          <ContactModal listing={listing} onClose={() => setShowContactModal(false)} onSend={handleSendMessage} />
        )}
      </div>
    </PublicLayout>
  );
}