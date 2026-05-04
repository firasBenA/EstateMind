/**
 * frontend-client/src/pages/dashboard/FraudCenter.tsx
 * Dashboard pour l'analyse multimodale (sans DashboardLayout)
 */
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
import { RefreshCw, ShieldAlert, ShieldCheck, AlertTriangle, Eye, Loader2 } from "lucide-react";

// ── Types (identiques à avant) ──
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

const RISK_COLORS: Record<string, string> = {
  incoherent: "#ef4444",
  suspect:    "#f97316",
  coherent:   "#22c55e",
};

const PIE_COLORS = ["#ef4444", "#f97316", "#22c55e"];

function riskBadge(level: string) {
  const label: Record<string, string> = {
    incoherent: "Incoherent",
    suspect:    "Suspect",
    coherent:   "Coherent",
  };
  return (
    <Badge variant="outline" className={
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

function KpiCard({ title, value, sub, icon, color }: {
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

export default function FraudCenter() {
  const [summary, setSummary] = useState<FraudSummary | null>(null);
  const [flags, setFlags] = useState<FlagStat[]>([]);
  const [listings, setListings] = useState<FraudListingsResponse | null>(null);
  const [riskFilter, setRiskFilter] = useState<string>("incoherent");
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const fetchAll = async () => {
    setRefreshing(true);
    try {
      const [sumRes, flagRes] = await Promise.all([
        fetch("/api/fraud/summary/", { credentials: "include" }),
        fetch("/api/fraud/flags/",   { credentials: "include" }),
      ]);
      if (sumRes.ok)  setSummary(await sumRes.json());
      if (flagRes.ok) setFlags((await flagRes.json()).flags ?? []);
    } catch (error) {
      console.error("Failed to fetch fraud data:", error);
    } finally {
      setRefreshing(false);
      setLoading(false);
    }
  };

  const fetchListings = async (risk: string, p: number) => {
    const params = new URLSearchParams({ risk, page: String(p), page_size: "15" });
    const res = await fetch(`/api/fraud/listings/?${params}`, { credentials: "include" });
    if (res.ok) setListings(await res.json());
  };

  useEffect(() => {
    fetchAll();
  }, []);

  useEffect(() => {
    fetchListings(riskFilter, page);
  }, [riskFilter, page]);

  const pieData = summary ? [
    { name: "Incoherent", value: summary.incoherent },
    { name: "Suspect",    value: summary.suspect },
    { name: "Coherent",   value: summary.coherent },
  ] : [];

  const isLoading = loading || refreshing;

  return (
    <div className="space-y-6">
      {/* Refresh button - compact */}
      <div className="flex justify-end">
        <Button variant="outline" size="sm" onClick={fetchAll} disabled={isLoading} className="gap-2">
          {isLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          Refresh
        </Button>
      </div>

      {/* KPI Cards */}
      {summary && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          <KpiCard title="Total Analyzed"  value={summary.total.toLocaleString()}
            icon={<ShieldCheck size={16} className="text-blue-600" />}
            color="bg-blue-50 dark:bg-blue-950" />
          <KpiCard title="Incoherent"     value={summary.incoherent.toLocaleString()}
            sub={`${summary.total ? Math.round(summary.incoherent / summary.total * 100) : 0}%`}
            icon={<ShieldAlert size={16} className="text-red-600" />}
            color="bg-red-50 dark:bg-red-950" />
          <KpiCard title="Suspect"        value={summary.suspect.toLocaleString()}
            sub={`${summary.total ? Math.round(summary.suspect / summary.total * 100) : 0}%`}
            icon={<AlertTriangle size={16} className="text-orange-500" />}
            color="bg-orange-50 dark:bg-orange-950" />
          <KpiCard title="Coherent"       value={summary.coherent.toLocaleString()}
            sub={`${summary.total ? Math.round(summary.coherent / summary.total * 100) : 0}%`}
            icon={<ShieldCheck size={16} className="text-green-600" />}
            color="bg-green-50 dark:bg-green-950" />
          <KpiCard title="Avg Score"     value={summary.avg_score.toFixed(3)}
            sub="multimodal [0–1]"
            icon={<Eye size={16} className="text-purple-600" />}
            color="bg-purple-50 dark:bg-purple-950" />
          <KpiCard title="Avg Price Dev." value={`${summary.avg_price_deviation.toFixed(1)}%`}
            sub="vs regional median"
            icon={<AlertTriangle size={16} className="text-yellow-600" />}
            color="bg-yellow-50 dark:bg-yellow-950" />
        </div>
      )}

      {/* Loading state for KPIs */}
      {isLoading && !summary && (
        <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
          {[1,2,3,4,5,6].map(i => (
            <Card key={i}>
              <CardContent className="p-4">
                <div className="h-16 bg-muted animate-pulse rounded" />
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Charts row */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        {summary && summary.total > 0 && (
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Risk Distribution</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={250}>
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

        {flags.length > 0 && (
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Top Detected Anomalies</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={flags.slice(0, 8)} layout="vertical"
                          margin={{ left: 10, right: 20, top: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                  <XAxis type="number" tick={{ fontSize: 11 }} />
                  <YAxis type="category" dataKey="flag" width={140}
                         tick={{ fontSize: 10 }}
                         tickFormatter={flagLabel} />
                  <Tooltip formatter={(v) => [v, "listings"]}
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
            <CardTitle className="text-sm">Listings by Risk Level</CardTitle>
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
                  {r === "incoherent" ? "Incoherent" :
                   r === "suspect"    ? "Suspect"    : "Coherent"}
                </button>
              ))}
            </div>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/30 text-left text-xs uppercase tracking-wider text-muted-foreground">
                  <th className="px-4 py-2.5">Listing</th>
                  <th className="px-3 py-2.5">Risk</th>
                  <th className="px-3 py-2.5">Score</th>
                  <th className="px-3 py-2.5">Price Dev.</th>
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
                      No listings in this category.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {listings && listings.pages > 1 && (
            <div className="flex items-center justify-between border-t border-border px-4 py-3">
              <p className="text-xs text-muted-foreground">
                {listings.count.toLocaleString()} listing{listings.count !== 1 ? "s" : ""}
              </p>
              <div className="flex gap-1">
                <Button variant="outline" size="sm" disabled={page <= 1}
                        onClick={() => setPage((p) => p - 1)} className="h-7 px-3 text-xs">
                  Previous
                </Button>
                <span className="flex items-center px-2 text-xs text-muted-foreground">
                  {page} / {listings.pages}
                </span>
                <Button variant="outline" size="sm" disabled={page >= listings.pages}
                        onClick={() => setPage((p) => p + 1)} className="h-7 px-3 text-xs">
                  Next
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Loading overlay for refresh */}
      {refreshing && (
        <div className="fixed inset-0 bg-background/50 flex items-center justify-center z-50">
          <div className="bg-card rounded-lg p-4 shadow-lg flex items-center gap-3">
            <Loader2 className="h-5 w-5 animate-spin text-primary" />
            <span className="text-sm">Refreshing fraud data...</span>
          </div>
        </div>
      )}
    </div>
  );
}