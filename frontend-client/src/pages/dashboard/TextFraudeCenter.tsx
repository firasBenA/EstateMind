/**
 * frontend-client/src/pages/dashboard/TextFraudCenter.tsx
 * Dashboard pour l'analyse textuelle des descriptions
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
import { RefreshCw, ShieldAlert, ShieldCheck, AlertTriangle, Eye, Loader2, FileText, Brain, Star } from "lucide-react";

// ── Types ──────────────────────────────────────────────────────────────────────

interface TextAnalysisSummary {
  total_analyzed: number;
  negatif: number;
  neutre_negatif: number;
  neutre_positif: number;
  positif: number;
  avg_score: number;
  avg_sentiment_score: number;
  avg_zeroshot_score: number;
  skipped_count: number;
}

interface TextAnalysisListing {
  listing_id: string;
  title: string;
  source_name: string;
  city: string;
  property_type: string;
  score_final: number;
  label_final: string;
  sentiment_stars: number;
  sentiment_score: number;
  zeroshot_label: string;
  zeroshot_score: number;
  rules_details: string;
  rules_count: number;
  nb_emojis: number;
  analyzed_at: string;
  url: string;
}

interface TextListingsResponse {
  count: number;
  pages: number;
  page: number;
  results: TextAnalysisListing[];
}

interface RuleStat {
  rule: string;
  count: number;
}

// ── Constants ─────────────────────────────────────────────────────────────────

const RISK_LABELS: Record<string, string> = {
  negatif: "Très suspect",
  neutre_negatif: "Suspect",
  neutre_positif: "Modéré",
  positif: "Fiable",
};

const PIE_COLORS = ["#ef4444", "#f97316", "#eab308", "#22c55e"];

// ── Helper functions ──────────────────────────────────────────────────────────

function riskBadge(level: string) {
  return (
    <Badge variant="outline" className={
      level === "negatif" ? "bg-red-100 text-red-700 border-red-200" :
      level === "neutre_negatif" ? "bg-orange-100 text-orange-700 border-orange-200" :
      level === "neutre_positif" ? "bg-yellow-100 text-yellow-700 border-yellow-200" :
      "bg-green-100 text-green-700 border-green-200"
    }>
      {RISK_LABELS[level] ?? level}
    </Badge>
  );
}

function getScoreColor(score: number): string {
  if (score < 0.3) return "text-red-600 font-bold";
  if (score < 0.6) return "text-orange-500 font-semibold";
  return "text-green-600";
}

// ── Sub-components ────────────────────────────────────────────────────────────

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

function TextListingRow({ listing }: { listing: TextAnalysisListing }) {
  const scoreColor = getScoreColor(listing.score_final);
  const confidencePercent = Math.round((1 - listing.score_final) * 100);

  return (
    <tr className="border-b border-border/50 hover:bg-muted/30 transition-colors">
      <td className="py-2.5 pr-3 max-w-[200px]">
        <p className="truncate text-sm font-medium text-foreground">
          {listing.title || listing.listing_id}
        </p>
        <p className="text-xs text-muted-foreground">{listing.city} · {listing.property_type}</p>
      </td>
      <td className="py-2.5 pr-3">{riskBadge(listing.label_final)}</td>
      <td className={`py-2.5 pr-3 text-sm ${scoreColor}`}>{confidencePercent}%</td>
      <td className="py-2.5 pr-3 text-sm text-muted-foreground">{listing.sentiment_stars}/5</td>
      <td className="py-2.5 pr-3 text-sm text-muted-foreground">
        <Badge variant="outline" className="text-xs">{listing.zeroshot_label}</Badge>
      </td>
      <td className="py-2.5 pr-3 max-w-[180px]">
        <div className="flex flex-wrap gap-1">
          {listing.rules_details && listing.rules_details !== "aucun" && (
            listing.rules_details.split("|").slice(0, 2).map((rule, idx) => (
              <span key={idx} className="rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">
                {rule.replace(/_/g, " ")}
              </span>
            ))
          )}
          {listing.rules_count > 2 && (
            <span className="rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">
              +{listing.rules_count - 2}
            </span>
          )}
        </div>
      </td>
      <td className="py-2.5 text-xs text-muted-foreground">{listing.nb_emojis} émojis</td>
      {listing.url && (
        <td className="py-2.5 pl-2">
          <a href={listing.url} target="_blank" rel="noreferrer" className="text-primary hover:underline">
            <Eye size={14} />
          </a>
        </td>
      )}
    </tr>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function TextFraudCenter() {
  const [summary, setSummary] = useState<TextAnalysisSummary | null>(null);
  const [rules, setRules] = useState<RuleStat[]>([]);
  const [listings, setListings] = useState<TextListingsResponse | null>(null);
  const [riskFilter, setRiskFilter] = useState<string>("negatif");
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const fetchAll = async () => {
    setRefreshing(true);
    try {
      const [sumRes, rulesRes] = await Promise.all([
        fetch("/api/fraud/text-summary/", { credentials: "include" }),
        fetch("/api/fraud/text-rules/", { credentials: "include" }),
      ]);
      if (sumRes.ok) setSummary(await sumRes.json());
      if (rulesRes.ok) setRules((await rulesRes.json()).rules ?? []);
    } catch (error) {
      console.error("Failed to fetch text fraud data:", error);
    } finally {
      setRefreshing(false);
      setLoading(false);
    }
  };

  const fetchListings = async (risk: string, p: number) => {
    const params = new URLSearchParams({ risk, page: String(p), page_size: "15" });
    const res = await fetch(`/api/fraud/text-listings/?${params}`, { credentials: "include" });
    if (res.ok) setListings(await res.json());
  };

  useEffect(() => {
    fetchAll();
  }, []);

  useEffect(() => {
    fetchListings(riskFilter, page);
  }, [riskFilter, page]);

  const pieData = summary ? [
    { name: "Très suspect", value: summary.negatif },
    { name: "Suspect", value: summary.neutre_negatif },
    { name: "Modéré", value: summary.neutre_positif },
    { name: "Fiable", value: summary.positif },
  ] : [];

  const scoreDistribution = summary ? [
    { range: "0-20%", count: summary.negatif },
    { range: "21-40%", count: Math.round(summary.neutre_negatif * 0.7) },
    { range: "41-60%", count: Math.round(summary.neutre_positif * 0.5) },
    { range: "61-80%", count: Math.round(summary.positif * 0.6) },
    { range: "81-100%", count: Math.round(summary.positif * 0.4) },
  ] : [];

  const isLoading = loading || refreshing;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold">Text Analysis Center</h1>
          <p className="text-sm text-muted-foreground">
            BERT sentiment + Zero-shot classification + Rule-based detection
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={fetchAll} disabled={isLoading} className="gap-2">
          {isLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          Refresh
        </Button>
      </div>

      {/* KPI Cards */}
      {summary && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-7">
          <KpiCard title="Total Analyzed" value={summary.total_analyzed.toLocaleString()}
            icon={<FileText size={16} className="text-blue-600" />}
            color="bg-blue-50 dark:bg-blue-950" />
          <KpiCard title="Skipped" value={summary.skipped_count.toLocaleString()}
            sub="Short or non-French"
            icon={<AlertTriangle size={16} className="text-gray-500" />}
            color="bg-gray-50 dark:bg-gray-950" />
          <KpiCard title="Très suspect" value={summary.negatif.toLocaleString()}
            sub={`${summary.total_analyzed ? Math.round(summary.negatif / summary.total_analyzed * 100) : 0}%`}
            icon={<ShieldAlert size={16} className="text-red-600" />}
            color="bg-red-50 dark:bg-red-950" />
          <KpiCard title="Suspect" value={summary.neutre_negatif.toLocaleString()}
            sub={`${summary.total_analyzed ? Math.round(summary.neutre_negatif / summary.total_analyzed * 100) : 0}%`}
            icon={<AlertTriangle size={16} className="text-orange-500" />}
            color="bg-orange-50 dark:bg-orange-950" />
          <KpiCard title="Fiable" value={summary.positif.toLocaleString()}
            sub={`${summary.total_analyzed ? Math.round(summary.positif / summary.total_analyzed * 100) : 0}%`}
            icon={<ShieldCheck size={16} className="text-green-600" />}
            color="bg-green-50 dark:bg-green-950" />
          <KpiCard title="Avg Risk Score" value={summary.avg_score.toFixed(3)}
            sub="0=fiable, 1=suspect"
            icon={<Brain size={16} className="text-purple-600" />}
            color="bg-purple-50 dark:bg-purple-950" />
          <KpiCard title="Avg Sentiment" value={summary.avg_sentiment_score.toFixed(2)}
            sub="BERT confidence"
            icon={<Star size={16} className="text-yellow-600" />}
            color="bg-yellow-50 dark:bg-yellow-950" />
        </div>
      )}

      {/* Loading state */}
      {isLoading && !summary && (
        <div className="grid grid-cols-2 md:grid-cols-7 gap-4">
          {[1, 2, 3, 4, 5, 6, 7].map(i => (
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
        {summary && summary.total_analyzed > 0 && (
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Risk Distribution (Text)</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie data={pieData} dataKey="value" nameKey="name"
                    cx="50%" cy="50%" outerRadius={80} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>
                    {pieData.map((_, i) => (<Cell key={i} fill={PIE_COLORS[i]} />))}
                  </Pie>
                  <Legend />
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        )}

        {scoreDistribution.length > 0 && (
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Confidence Score Distribution</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={scoreDistribution}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="range" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Bar dataKey="count" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        )}
      </div>

      {/* Rules bar chart */}
      {rules.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Most Detected Text Signals</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={rules.slice(0, 8)} layout="vertical"
                margin={{ left: 10, right: 20, top: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                <XAxis type="number" tick={{ fontSize: 11 }} />
                <YAxis type="category" dataKey="rule" width={140}
                  tick={{ fontSize: 10 }}
                  tickFormatter={(v) => v.replace(/_/g, " ")} />
                <Tooltip formatter={(v) => [v, "listings"]} />
                <Bar dataKey="count" fill="#f97316" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      )}

      {/* Listings table */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between flex-wrap gap-2">
            <CardTitle className="text-sm">Listings by Text Analysis</CardTitle>
            <div className="flex gap-1">
              {(["negatif", "neutre_negatif", "neutre_positif", "positif"] as const).map((r) => (
                <button key={r}
                  onClick={() => { setRiskFilter(r); setPage(1); }}
                  className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${riskFilter === r
                    ? r === "negatif" ? "bg-red-500 text-white"
                      : r === "neutre_negatif" ? "bg-orange-500 text-white"
                        : r === "neutre_positif" ? "bg-yellow-500 text-white"
                          : "bg-green-500 text-white"
                    : "bg-muted text-muted-foreground hover:text-foreground"
                    }`}>
                  {RISK_LABELS[r]}
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
                  <th className="px-3 py-2.5">Sentiment</th>
                  <th className="px-3 py-2.5">Class.</th>
                  <th className="px-3 py-2.5">Signals</th>
                  <th className="px-3 py-2.5">Émojis</th>
                  <th className="px-2 py-2.5"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/40">
                {listings?.results.map((l) => (
                  <TextListingRow key={`${l.source_name}_${l.listing_id}`} listing={l} />
                ))}
                {(!listings || listings.results.length === 0) && (
                  <tr>
                    <td colSpan={8} className="py-8 text-center text-sm text-muted-foreground">
                      No listings in this category.
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

      {/* Loading overlay */}
      {refreshing && (
        <div className="fixed inset-0 bg-background/50 flex items-center justify-center z-50">
          <div className="bg-card rounded-lg p-4 shadow-lg flex items-center gap-3">
            <Loader2 className="h-5 w-5 animate-spin text-primary" />
            <span className="text-sm">Refreshing text analysis data...</span>
          </div>
        </div>
      )}
    </div>
  );
}