import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle,
} from "@/components/ui/dialog";
import { TrendingUp, Loader2, AlertTriangle } from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from "recharts";

// ── Types ─────────────────────────────────────────────────────────────────────

interface ForecastPrediction {
  year: number;
  price: number;
  price_low: number;
  price_high: number;
  price_per_m2: number;
}

interface ForecastData {
  current_price: number;
  current_year: number;
  predictions: ForecastPrediction[];
  property_type: string;
  zone: string;
  source: string;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmtPrice(p: number | null): string {
  if (p == null) return "Prix sur demande";
  return p.toLocaleString("fr-TN") + " TND";
}

function fmtM2(v: number | null): string {
  if (!v || v <= 0) return "";
  return v.toLocaleString("fr-TN") + " TND/m²";
}

// ── Inner modal ───────────────────────────────────────────────────────────────

function ForecastModal({
  listingId,
  listingTitle,
  onClose,
}: {
  listingId: string;
  listingTitle: string;
  onClose: () => void;
}) {
  const [data, setData]               = useState<ForecastData | null>(null);
  const [loading, setLoading]         = useState(true);
  const [error, setError]             = useState<string | null>(null);
  const [hoveredYear, setHoveredYear] = useState<number | null>(null);

  useEffect(() => {
    fetch(`/api/listings/${listingId}/forecast/`)
      .then((r) => r.json())
      .then((json) => {
        if (json.success) {
          setData(json.data);
          setHoveredYear(json.data.predictions[0]?.year ?? null);
        } else {
          setError(json.error || "Prévisions indisponibles pour cette annonce.");
        }
      })
      .catch(() => setError("Erreur réseau lors du chargement des prévisions."))
      .finally(() => setLoading(false));
  }, [listingId]);

  const chartData = data
    ? [
        {
          year: data.current_year,
          price: data.current_price,
          price_low: data.current_price,
          price_high: data.current_price,
        },
        ...data.predictions,
      ]
    : [];

  const active = data?.predictions.find((p) => p.year === hoveredYear) ?? null;

  return (
    <Dialog open onOpenChange={onClose}>
      <DialogContent className="sm:max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-primary" />
            Prévisions des prix 2027–2032
          </DialogTitle>
          <p className="text-sm text-muted-foreground line-clamp-1">{listingTitle}</p>
        </DialogHeader>

        {/* Loading */}
        {loading && (
          <div className="flex justify-center items-center py-16 gap-3">
            <Loader2 className="h-7 w-7 animate-spin text-primary" />
            <span className="text-sm text-muted-foreground">Chargement des prévisions…</span>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="text-center py-10 space-y-2">
            <AlertTriangle className="h-8 w-8 text-amber-500 mx-auto" />
            <p className="text-sm text-muted-foreground">{error}</p>
          </div>
        )}

        {/* Data */}
        {data && (
          <div className="space-y-5 pt-2">
            {/* Current price banner */}
            <div className="flex items-center justify-between rounded-xl bg-muted/50 px-4 py-3">
              <span className="text-sm text-muted-foreground">
                Prix actuel ({data.current_year})
              </span>
              <span className="text-xl font-bold text-primary">
                {fmtPrice(data.current_price)}
              </span>
            </div>

            {/* Chart */}
            <div className="h-52">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="year" tick={{ fontSize: 11 }} />
                  <YAxis
                    tick={{ fontSize: 11 }}
                    tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`}
                    width={52}
                  />
                  <Tooltip
                    labelFormatter={(l) => `Année ${l}`}
                    formatter={(v: number, name: string) => {
                      const labels: Record<string, string> = {
                        price: "Prix prévu",
                        price_low: "Borne basse",
                        price_high: "Borne haute",
                      };
                      return [fmtPrice(Math.round(v)), labels[name] ?? name];
                    }}
                  />
                  <ReferenceLine
                    x={data.current_year}
                    stroke="hsl(var(--muted-foreground))"
                    strokeDasharray="4 4"
                    label={{ value: "Aujourd'hui", position: "insideTopRight", fontSize: 10 }}
                  />
                  <Line
                    type="monotone"
                    dataKey="price_high"
                    stroke="hsl(var(--primary))"
                    strokeWidth={1}
                    strokeDasharray="5 5"
                    dot={false}
                    strokeOpacity={0.4}
                  />
                  <Line
                    type="monotone"
                    dataKey="price_low"
                    stroke="hsl(var(--primary))"
                    strokeWidth={1}
                    strokeDasharray="5 5"
                    dot={false}
                    strokeOpacity={0.4}
                  />
                  <Line
                    type="monotone"
                    dataKey="price"
                    stroke="hsl(var(--primary))"
                    strokeWidth={2.5}
                    dot={{ r: 4, fill: "hsl(var(--primary))" }}
                    activeDot={{ r: 6 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Interactive timeline */}
            <div className="relative pt-3 pb-1">
              <div className="absolute top-[18px] left-[5%] right-[5%] h-0.5 bg-border rounded-full" />
              <div className="flex justify-around relative">
                {data.predictions.map((p) => (
                  <button
                    key={p.year}
                    onMouseEnter={() => setHoveredYear(p.year)}
                    onFocus={() => setHoveredYear(p.year)}
                    onClick={() => setHoveredYear(p.year)}
                    className="flex flex-col items-center gap-1.5 group outline-none"
                  >
                    <div
                      className={`w-5 h-5 rounded-full border-2 transition-all duration-200 ${
                        hoveredYear === p.year
                          ? "bg-primary border-primary scale-125 shadow-md shadow-primary/30"
                          : "bg-background border-border group-hover:border-primary group-hover:scale-110"
                      }`}
                    />
                    <span
                      className={`text-xs font-medium transition-colors ${
                        hoveredYear === p.year ? "text-primary" : "text-muted-foreground"
                      }`}
                    >
                      {p.year}
                    </span>
                  </button>
                ))}
              </div>
            </div>

            {/* Detail card */}
            {active && (
              <div className="rounded-xl border border-primary/20 bg-primary/5 p-4 space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-semibold">Prix prévu en {active.year}</span>
                  <span className="text-2xl font-bold text-primary">{fmtPrice(active.price)}</span>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <div className="rounded-lg bg-background/70 px-3 py-2">
                    <div className="text-[10px] uppercase tracking-wide text-muted-foreground mb-0.5">
                      Borne basse
                    </div>
                    <div className="text-sm font-semibold">{fmtPrice(active.price_low)}</div>
                  </div>
                  <div className="rounded-lg bg-background/70 px-3 py-2">
                    <div className="text-[10px] uppercase tracking-wide text-muted-foreground mb-0.5">
                      Borne haute
                    </div>
                    <div className="text-sm font-semibold">{fmtPrice(active.price_high)}</div>
                  </div>
                </div>
                {active.price_per_m2 > 0 && (
                  <p className="text-xs text-muted-foreground">
                    Prix/m² estimé :{" "}
                    <span className="font-semibold text-foreground">{fmtM2(active.price_per_m2)}</span>
                  </p>
                )}
                {data.current_price > 0 && (
                  <p className="text-xs flex items-center gap-1">
                    <span className="text-muted-foreground">Variation vs {data.current_year} :</span>
                    <span
                      className={`font-bold ${
                        active.price >= data.current_price ? "text-green-600" : "text-red-500"
                      }`}
                    >
                      {active.price >= data.current_price ? "+" : ""}
                      {(
                        ((active.price - data.current_price) / data.current_price) *
                        100
                      ).toFixed(1)}
                      %
                    </span>
                  </p>
                )}
              </div>
            )}

            <p className="text-xs text-center text-muted-foreground pb-1">
              Modèle hybride IPIM · Données 2026 · Survolez une année pour voir les détails
            </p>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

// ── Public export ─────────────────────────────────────────────────────────────

/**
 * Self-contained Forecasting button + modal.
 * variant="card"   → small inline button (for ListingCard)
 * variant="detail" → full-width button (for ListingDetailPage)
 *
 * Always calls e.stopPropagation() so it works safely inside <Link> wrappers.
 */
export function ForecastingButton({
  listing,
  variant = "card",
  className,
}: {
  listing: { id: string; title?: string | null };
  variant?: "card" | "detail";
  className?: string;
}) {
  const [open, setOpen] = useState(false);

  function handleOpen(e: React.MouseEvent) {
    e.preventDefault();
    e.stopPropagation();
    setOpen(true);
  }

  return (
    <>
      {variant === "detail" ? (
        <Button
          size="lg"
          variant="outline"
          className={`w-full gap-2 border-primary/40 text-primary hover:bg-primary/10 ${className ?? ""}`}
          onClick={handleOpen}
        >
          <TrendingUp className="h-4 w-4" />
          Forecasting 2027–2032
        </Button>
      ) : (
        <button
          onClick={handleOpen}
          className={`inline-flex items-center gap-1 rounded border border-primary/40 px-2 py-0.5 text-[11px] font-medium text-primary hover:bg-primary/10 transition-colors ${className ?? ""}`}
        >
          <TrendingUp className="h-3 w-3" />
          Forecasting
        </button>
      )}

      {open && (
        <ForecastModal
          listingId={listing.id}
          listingTitle={listing.title ?? ""}
          onClose={() => setOpen(false)}
        />
      )}
    </>
  );
}
