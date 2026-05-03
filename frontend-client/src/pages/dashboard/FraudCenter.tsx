import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend,
} from "recharts";
import {
  Card, CardContent, CardHeader, CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCw, ShieldAlert, ShieldCheck, AlertTriangle, Eye } from "lucide-react";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ChevronDown, ChevronUp } from "lucide-react";

// ── Types ──────────────────────────────────────────────────────────────────────

interface FraudSummary {
  total: number;
  incoherent: number;
  suspect: number;
  coherent: number;
  avg_score: number;
  avg_price_deviation: number;
}

interface FraudListing {
  property_id: string;
  source_name: string;
  multimodal_score: number;
  risk_level: "incoherent" | "suspect" | "coherent";
  price_deviation_pct: number;
  mismatch_types: string[];
  images_analyzed: number;
  analyzed_at: string | null;
  title: string;
  price: number | null;
  city: string;
  region: string;
  property_type: string;
  url: string;
}

interface FraudListingsResponse {
  count: number;
  pages: number;
  page: number;
  results: FraudListing[];
}

interface FlagStat {
  flag: string;
  count: number;
}

// ── Helpers ────────────────────────────────────────────────────────────────────

const RISK_COLORS: Record<string, string> = {
  incoherent: "#ef4444",
  suspect:    "#f97316",
  coherent:   "#22c55e",
};

const PIE_COLORS = ["#ef4444", "#f97316", "#22c55e"];

function riskBadge(level: string) {
  const map: Record<string, string> = {
    incoherent: "destructive",
    suspect:    "secondary",
    coherent:   "outline",
  };
  const label: Record<string, string> = {
    incoherent: "Incohérent",
    suspect:    "Suspect",
    coherent:   "Cohérent",
  };
  return (
    <Badge variant={map[level] as any} className={
      level === "incoherent" ? "bg-red-100 text-red-700 border-red-200" :
      level === "suspect"    ? "bg-orange-100 text-orange-700 border-orange-200" :
                               "bg-green-100 text-green-700 border-green-200"
    }>
      {label[level] ?? level}
    </Badge>
  );
}

function flagLabel(flag: string): string {
  const map: Record<string, string> = {
    no_real_estate_images:      "No RE images",
    claimed_pool_not_visible:   "Pool missing",
    claimed_parking_not_visible:"Parking missing",
    claimed_garden_not_visible: "Garden missing",
    claimed_view_not_visible:   "View missing",
    claimed_terrace_not_visible:"Terrace missing",
    claimed_land_not_visible:   "Land mismatch",
    wrong_property_type:        "Wrong type",
    overpriced_vs_images:       "Overpriced",
    underpriced_trap:           "Underpriced trap",
    no_images_suspicious_price: "No images + price",
  };
  return map[flag] ?? flag;
}

// ── Sub-components ─────────────────────────────────────────────────────────────

function KpiCard({
  title, value, sub, icon, color,
}: {
  title: string; value: string | number; sub?: string;
  icon: React.ReactNode; color: string;
}) {
  return (
    <Card>
      <CardContent className="pt-5">
        <div className="flex items-center gap-3">
          <div className={`rounded-lg p-2 ${color}`}>{icon}</div>
          <div>
            <p className="text-2xl font-bold text-foreground">{value}</p>
            <p className="text-xs text-muted-foreground">{title}</p>
            {sub && <p className="text-xs text-muted-foreground">{sub}</p>}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function ListingRow({ listing }: { listing: FraudListing }) {
  const scoreColor =
    listing.multimodal_score < 0.31 ? "text-red-600 font-bold" :
    listing.multimodal_score < 0.56 ? "text-orange-500 font-semibold" :
                                       "text-green-600";
  return (
    <tr className="border-b border-border/50 hover:bg-muted/30 transition-colors">
      <td className="py-2.5 pr-3 max-w-[200px]">
        <p className="truncate text-sm font-medium text-foreground">
          {listing.title || listing.property_id}
        </p>
        <p className="text-xs text-muted-foreground">{listing.city} · {listing.property_type}</p>
      </td>
      <td className="py-2.5 pr-3">{riskBadge(listing.risk_level)}</td>
      <td className={`py-2.5 pr-3 text-sm ${scoreColor}`}>
        {listing.multimodal_score.toFixed(3)}
      </td>
      <td className="py-2.5 pr-3 text-sm text-muted-foreground">
        {listing.price_deviation_pct > 0 ? "+" : ""}{listing.price_deviation_pct}%
      </td>
      <td className="py-2.5 pr-3 max-w-[220px]">
        <div className="flex flex-wrap gap-1">
          {listing.mismatch_types.slice(0, 2).map((f) => (
            <span key={f} className="rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">
              {flagLabel(f)}
            </span>
          ))}
          {listing.mismatch_types.length > 2 && (
            <span className="rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">
              +{listing.mismatch_types.length - 2}
            </span>
          )}
        </div>
      </td>
      <td className="py-2.5 text-xs text-muted-foreground">{listing.images_analyzed} imgs</td>
      {listing.url && (
        <td className="py-2.5 pl-2">
          <a href={listing.url} target="_blank" rel="noreferrer"
             className="text-primary hover:underline">
            <Eye size={14} />
          </a>
        </td>
      )}
    </tr>
  );
}

// ── Main component ─────────────────────────────────────────────────────────────

export function FraudDashboard() {
  const [open, setOpen] = useState(false);
  const [summary, setSummary]       = useState<FraudSummary | null>(null);
  const [flags, setFlags]           = useState<FlagStat[]>([]);
  const [listings, setListings]     = useState<FraudListingsResponse | null>(null);
  const [riskFilter, setRiskFilter] = useState<string>("incoherent");
  const [page, setPage]             = useState(1);
  const [loading, setLoading]       = useState(false);

  const fetchAll = async () => {
    setLoading(true);
    try {
      const [sumRes, flagRes] = await Promise.all([
        fetch("/api/fraud/summary/", { credentials: "include" }),
        fetch("/api/fraud/flags/",   { credentials: "include" }),
      ]);
      if (sumRes.ok)  setSummary(await sumRes.json());
      if (flagRes.ok) setFlags((await flagRes.json()).flags ?? []);
    } finally {
      setLoading(false);
    }
  };

  const fetchListings = async (risk: string, p: number) => {
    const params = new URLSearchParams({ risk, page: String(p), page_size: "15" });
    const res = await fetch(`/api/fraud/listings/?${params}`, { credentials: "include" });
    if (res.ok) setListings(await res.json());
  };

  useEffect(() => {
    if (open) fetchAll();
  }, [open]);

  useEffect(() => {
    if (open) fetchListings(riskFilter, page);
  }, [open, riskFilter, page]);

  const pieData = summary ? [
    { name: "Incohérent", value: summary.incoherent },
    { name: "Suspect",    value: summary.suspect },
    { name: "Cohérent",   value: summary.coherent },
  ] : [];

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger asChild>
        <button className="w-full flex items-center justify-between rounded-xl bg-card border border-border px-5 py-4 text-left shadow-sm hover:bg-muted/30 transition-colors">
          <div className="flex items-center gap-3">
            <ShieldAlert className="text-destructive" size={20} />
            <div>
              <p className="font-semibold text-foreground">Fraud Detection — DSO 2.2</p>
              <p className="text-xs text-muted-foreground">CLIP Zero-Shot Semantic Analysis</p>
            </div>
            {summary && (
              <Badge variant="destructive" className="ml-2">
                {summary.incoherent} incohérents
              </Badge>
            )}
          </div>
          {open ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        </button>
      </CollapsibleTrigger>

      <CollapsibleContent className="space-y-6 pt-4 animate-fade-in">

        {/* KPI cards */}
        {summary && (
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
            <KpiCard title="Total analysés"  value={summary.total.toLocaleString()}
              icon={<ShieldCheck size={16} className="text-blue-600" />}
              color="bg-blue-50 dark:bg-blue-950" />
            <KpiCard title="Incohérents"     value={summary.incoherent.toLocaleString()}
              sub={`${summary.total ? Math.round(summary.incoherent / summary.total * 100) : 0}%`}
              icon={<ShieldAlert size={16} className="text-red-600" />}
              color="bg-red-50 dark:bg-red-950" />
            <KpiCard title="Suspects"        value={summary.suspect.toLocaleString()}
              sub={`${summary.total ? Math.round(summary.suspect / summary.total * 100) : 0}%`}
              icon={<AlertTriangle size={16} className="text-orange-500" />}
              color="bg-orange-50 dark:bg-orange-950" />
            <KpiCard title="Cohérents"       value={summary.coherent.toLocaleString()}
              sub={`${summary.total ? Math.round(summary.coherent / summary.total * 100) : 0}%`}
              icon={<ShieldCheck size={16} className="text-green-600" />}
              color="bg-green-50 dark:bg-green-950" />
            <KpiCard title="Score moyen"     value={summary.avg_score.toFixed(3)}
              sub="multimodal [0–1]"
              icon={<Eye size={16} className="text-purple-600" />}
              color="bg-purple-50 dark:bg-purple-950" />
            <KpiCard title="Écart prix moy." value={`${summary.avg_price_deviation.toFixed(1)}%`}
              sub="vs médiane régionale"
              icon={<AlertTriangle size={16} className="text-yellow-600" />}
              color="bg-yellow-50 dark:bg-yellow-950" />
          </div>
        )}

        {/* Charts row */}
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {/* Pie */}
          {summary && summary.total > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">Distribution des risques</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={220}>
                  <PieChart>
                    <Pie data={pieData} dataKey="value" nameKey="name"
                         cx="50%" cy="50%" outerRadius={80} label={
                           ({ name, percent }) =>
                             `${name} ${(percent * 100).toFixed(0)}%`
                         }>
                      {pieData.map((_, i) => (
                        <Cell key={i} fill={PIE_COLORS[i]} />
                      ))}
                    </Pie>
                    <Legend />
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          )}

          {/* Flags bar chart */}
          {flags.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">Top anomalies détectées</CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={flags.slice(0, 8)} layout="vertical"
                            margin={{ left: 10, right: 20, top: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" tick={{ fontSize: 11 }} />
                    <YAxis type="category" dataKey="flag" width={140}
                           tick={{ fontSize: 10 }}
                           tickFormatter={flagLabel} />
                    <Tooltip formatter={(v) => [v, "annonces"]}
                             labelFormatter={flagLabel} />
                    <Bar dataKey="count" fill="#ef4444" radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          )}
        </div>

        {/* Listings table */}
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between flex-wrap gap-2">
              <CardTitle className="text-sm">Annonces par niveau de risque</CardTitle>
              <div className="flex gap-1">
                {(["incoherent", "suspect", "coherent"] as const).map((r) => (
                  <button key={r}
                    onClick={() => { setRiskFilter(r); setPage(1); }}
                    className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                      riskFilter === r
                        ? r === "incoherent" ? "bg-red-500 text-white"
                        : r === "suspect"    ? "bg-orange-500 text-white"
                                             : "bg-green-500 text-white"
                        : "bg-muted text-muted-foreground hover:text-foreground"
                    }`}>
                    {r === "incoherent" ? "Incohérents" :
                     r === "suspect"    ? "Suspects"    : "Cohérents"}
                  </button>
                ))}
                <Button variant="ghost" size="sm" onClick={() => fetchListings(riskFilter, page)}
                        className="h-7 px-2">
                  <RefreshCw size={13} />
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/30 text-left text-xs uppercase tracking-wider text-muted-foreground">
                    <th className="px-4 py-2.5">Annonce</th>
                    <th className="px-3 py-2.5">Risque</th>
                    <th className="px-3 py-2.5">Score</th>
                    <th className="px-3 py-2.5">Écart prix</th>
                    <th className="px-3 py-2.5">Anomalies</th>
                    <th className="px-3 py-2.5">Images</th>
                    <th className="px-2 py-2.5"></th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/40">
                  {listings?.results.map((l) => (
                    <ListingRow key={`${l.source_name}_${l.property_id}`} listing={l} />
                  ))}
                  {listings?.results.length === 0 && (
                    <tr>
                      <td colSpan={7} className="py-8 text-center text-sm text-muted-foreground">
                        Aucune annonce dans cette catégorie.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            {listings && listings.pages > 1 && (
              <div className="flex items-center justify-between border-t border-border px-4 py-3">
                <p className="text-xs text-muted-foreground">
                  {listings.count.toLocaleString()} annonce{listings.count !== 1 ? "s" : ""}
                </p>
                <div className="flex gap-1">
                  <Button variant="outline" size="sm" disabled={page <= 1}
                          onClick={() => setPage((p) => p - 1)} className="h-7 px-3 text-xs">
                    Préc.
                  </Button>
                  <span className="flex items-center px-2 text-xs text-muted-foreground">
                    {page} / {listings.pages}
                  </span>
                  <Button variant="outline" size="sm" disabled={page >= listings.pages}
                          onClick={() => setPage((p) => p + 1)} className="h-7 px-3 text-xs">
                    Suiv.
                  </Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {loading && (
          <div className="flex justify-center py-4">
            <div className="h-6 w-6 animate-spin rounded-full border-2 border-primary border-t-transparent" />
          </div>
        )}

      </CollapsibleContent>
    </Collapsible>
  );
}
