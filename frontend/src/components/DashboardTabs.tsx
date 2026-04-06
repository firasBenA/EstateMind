
import { useState, useEffect } from "react";
import { formatDistanceToNow } from "date-fns";

type TabId = "agent" | "listings" | "scrapers";

const TABS: { id: TabId; label: string }[] = [
  { id: "agent", label: "Agent Dashboard" },
  { id: "listings", label: "Listings" },
  { id: "scrapers", label: "Scrapers" },
];

interface MetricsData {
  total_listings: number;
  latest_run: {
    source_name: string | null;
    strategy: string | null;
    fetched: number | null;
    inserted: number | null;
    updated: number | null;
    unchanged: number | null;
    errors: number | null;
    started_at: string | null;
    finished_at: string | null;
  };
  per_source: {
    source_name: string;
    count: number;
  }[];
  recent_runs: {
    source_name: string;
    fetched: number;
    inserted: number;
    updated: number;
    unchanged: number;
    errors: number;
    started_at: string | null;
  }[];
}

const COLORS = [
  "bg-primary",
  "bg-accent-foreground",
  "bg-success",
  "bg-warning",
  "bg-destructive",
  "bg-muted-foreground",
];

function AgentTab({ data }: { data: MetricsData | null }) {
  if (!data || !data.latest_run.started_at) {
    return (
      <div className="glass-panel rounded-xl p-5 text-center text-muted-foreground">
        No run data available.
      </div>
    );
  }

  const run = data.latest_run;
  const duration =
    run.started_at && run.finished_at
      ? formatDistanceToNow(new Date(run.started_at)) // Approximate
      : "In Progress";
  
  // Calculate success rate if possible
  const totalProcessed = (run.fetched || 0);
  const successRate = totalProcessed > 0 
    ? `${Math.round(((totalProcessed - (run.errors || 0)) / totalProcessed) * 100)}%` 
    : "N/A";

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="glass-panel rounded-xl p-5">
        <h3 className="text-sm font-semibold text-foreground mb-3">Latest Run Summary</h3>
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-5">
          {[
            { label: "Duration", value: duration },
            { label: "Pages Crawled", value: (run.fetched || 0).toLocaleString() },
            { label: "New Records", value: (run.inserted || 0).toLocaleString() },
            { label: "Updated Records", value: (run.updated || 0).toLocaleString() },
            { label: "Success Rate", value: successRate },
          ].map((item) => (
            <div key={item.label}>
              <p className="text-xs text-muted-foreground">{item.label}</p>
              <p className="text-lg font-bold text-foreground">{item.value}</p>
            </div>
          ))}
        </div>
      </div>
      <div className="glass-panel rounded-xl p-5">
        <h3 className="text-sm font-semibold text-foreground mb-3">Run Details</h3>
        <div className="space-y-2 text-sm">
          {[
            { label: "Started", value: run.started_at ? new Date(run.started_at).toLocaleString() : "-" },
            { label: "Finished", value: run.finished_at ? new Date(run.finished_at).toLocaleString() : "-" },
            { label: "Agent", value: run.source_name || "Unknown" },
            { label: "Mode", value: run.strategy || "Standard" },
          ].map((row) => (
            <div key={row.label} className="flex justify-between border-b border-border/50 pb-2 last:border-0">
              <span className="text-muted-foreground">{row.label}</span>
              <span className="font-medium text-foreground">{row.value}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function ListingsTab({ data }: { data: MetricsData | null }) {
  if (!data) return null;

  const total = data.total_listings;

  return (
    <div className="glass-panel rounded-xl p-5 animate-fade-in">
      <h3 className="text-sm font-semibold text-foreground mb-4">Distribution by Source</h3>
      <div className="space-y-3">
        {data.per_source.map((src, i) => {
          const pct = total > 0 ? Math.round((src.count / total) * 100) : 0;
          const color = COLORS[i % COLORS.length];
          return (
            <div key={src.source_name}>
              <div className="flex items-center justify-between text-sm mb-1">
                <span className="text-foreground font-medium">{src.source_name}</span>
                <span className="text-muted-foreground">
                  {src.count.toLocaleString()} ({pct}%)
                </span>
              </div>
              <div className="h-2 w-full rounded-full bg-secondary">
                <div
                  className={`h-full rounded-full ${color} transition-all duration-700`}
                  style={{ width: `${pct}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ScrapersTab({ data }: { data: MetricsData | null }) {
  if (!data) return null;

  return (
    <div className="glass-panel rounded-xl p-5 animate-fade-in">
      <h3 className="text-sm font-semibold text-foreground mb-4">Recent Runs</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs uppercase tracking-wider text-muted-foreground">
              <th className="pb-2 pr-4">Scraper</th>
              <th className="pb-2 pr-4">Status</th>
              <th className="pb-2 pr-4">Fetched</th>
              <th className="pb-2 pr-4">Inserted</th>
              <th className="pb-2 pr-4">Updated</th>
              <th className="pb-2 pr-4">Errors</th>
              <th className="pb-2">Time</th>
            </tr>
          </thead>
          <tbody>
            {data.recent_runs.map((run, i) => (
              <tr key={i} className="border-b border-border/50 last:border-0">
                <td className="py-2.5 pr-4 font-medium text-foreground">{run.source_name}</td>
                <td className="py-2.5 pr-4">
                  <span
                    className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                      run.errors === 0
                        ? "bg-success/10 text-success"
                        : "bg-destructive/10 text-destructive"
                    }`}
                  >
                    {run.errors === 0 ? "success" : "warning"}
                  </span>
                </td>
                <td className="py-2.5 pr-4 text-muted-foreground">{run.fetched}</td>
                <td className="py-2.5 pr-4 text-muted-foreground">{run.inserted}</td>
                <td className="py-2.5 pr-4 text-muted-foreground">{run.updated}</td>
                <td className="py-2.5 pr-4 text-muted-foreground">{run.errors}</td>
                <td className="py-2.5 text-muted-foreground">
                    {run.started_at ? formatDistanceToNow(new Date(run.started_at)) + " ago" : "-"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function DashboardTabs() {
  const [active, setActive] = useState<TabId>("agent");
  const [data, setData] = useState<MetricsData | null>(null);

  useEffect(() => {
    fetch("/api/metrics/", { credentials: "include" })
      .then((res) => (res.ok ? res.json() : null))
      .then(setData)
      .catch(console.error);
  }, []);

  return (
    <div>
      <div className="flex gap-1 rounded-lg bg-secondary/50 p-1 mb-4">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActive(tab.id)}
            className={`flex-1 rounded-md px-3 py-2 text-sm font-medium transition-all ${
              active === tab.id
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {active === "agent" && <AgentTab data={data} />}
      {active === "listings" && <ListingsTab data={data} />}
      {active === "scrapers" && <ScrapersTab data={data} />}
    </div>
  );
}
