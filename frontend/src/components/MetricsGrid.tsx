import { useEffect, useState } from "react";
import { BarChart3, Download, AlertTriangle, Database, CheckCircle, XCircle, TrendingUp } from "lucide-react";

interface Metrics {
  total_listings: number;
  last_run_fetched: number;
  inserted: number;
  errors: number;
  flagged_count: number;
  high_quality: number;
  drop_quality: number;
}

interface ScoreDistItem {
  level: string;
  count: number;
}

const DEFAULT_METRICS: Metrics = {
  total_listings: 0,
  last_run_fetched: 0,
  inserted: 0,
  errors: 0,
  flagged_count: 0,
  high_quality: 0,
  drop_quality: 0,
};

export function MetricsGrid() {
  const [metrics, setMetrics] = useState<Metrics>(DEFAULT_METRICS);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch("/api/metrics/", { credentials: "include" }).then(r => r.ok ? r.json() : null),
      fetch("/api/quality/", { credentials: "include" }).then(r => r.ok ? r.json() : null),
    ]).then(([metricsData, qualityData]) => {
      if (metricsData || qualityData) {
        const scoreDist: ScoreDistItem[] = qualityData?.score_distribution || [];
        const high = scoreDist.find((s) => s.level === "HIGH")?.count || 0;
        const drop = scoreDist.find((s) => s.level === "DROP")?.count || 0;
        setMetrics({
          total_listings: metricsData?.total_listings || 0,
          last_run_fetched: metricsData?.latest_run?.fetched || 0,
          inserted: metricsData?.latest_run?.inserted || 0,
          errors: qualityData?.outlier_count || metricsData?.latest_run?.errors || 0,
          flagged_count: qualityData?.outlier_count || 0,
          high_quality: high,
          drop_quality: drop,
        });
      }
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const cards = [
    {
      label: "Total Listings",
      value: metrics.total_listings.toLocaleString(),
      icon: Database,
      accent: "text-primary",
      bgAccent: "bg-primary/10",
      sub: "in Pinecone",
    },
    {
      label: "Last Run Fetched",
      value: metrics.last_run_fetched.toLocaleString(),
      icon: Download,
      accent: "text-accent-foreground",
      bgAccent: "bg-accent",
      sub: "listings scraped",
    },
    {
      label: "High Quality",
      value: metrics.high_quality.toLocaleString(),
      icon: CheckCircle,
      accent: "text-success",
      bgAccent: "bg-success/10",
      sub: "score ≥ 85",
    },
    {
      label: "Dropped",
      value: metrics.drop_quality.toLocaleString(),
      icon: XCircle,
      accent: "text-muted-foreground",
      bgAccent: "bg-muted",
      sub: "score < 25",
    },
    {
      label: "Outliers Flagged",
      value: metrics.flagged_count.toLocaleString(),
      icon: AlertTriangle,
      accent: "text-warning",
      bgAccent: "bg-warning/10",
      sub: "anomalies detected",
    },
    {
      label: "Inserted",
      value: metrics.inserted.toLocaleString(),
      icon: TrendingUp,
      accent: "text-primary",
      bgAccent: "bg-primary/10",
      sub: "new this run",
    },
  ];

  return (
    <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
      {cards.map((card, i) => (
        <div
          key={card.label}
          className="glass-panel rounded-xl p-4 animate-slide-up"
          style={{ animationDelay: `${i * 60}ms`, animationFillMode: "both" }}
        >
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-medium uppercase tracking-wider text-muted-foreground truncate">
              {card.label}
            </span>
            <div className={`flex h-7 w-7 shrink-0 items-center justify-center rounded-lg ${card.bgAccent}`}>
              <card.icon className={`h-3.5 w-3.5 ${card.accent}`} />
            </div>
          </div>
          <p className={`text-2xl font-bold tracking-tight text-foreground ${loading ? "opacity-30" : ""}`}>
            {loading ? "—" : card.value}
          </p>
          <p className="text-xs text-muted-foreground mt-1">{card.sub}</p>
        </div>
      ))}
    </div>
  );
}
