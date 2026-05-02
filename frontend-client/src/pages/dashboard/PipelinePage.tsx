import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { CheckCircle, Clock, XCircle, Zap, Loader2, RefreshCw } from "lucide-react";
import { useMetrics, useQuality } from "@/hooks/useAdminData";

function formatDate(s: string | null) {
  if (!s) return "—";
  return new Date(s).toLocaleString("fr-TN");
}

function duration(start: string | null, end: string | null) {
  if (!start || !end) return "—";
  const ms = new Date(end).getTime() - new Date(start).getTime();
  return ms < 60_000 ? `${(ms / 1000).toFixed(1)}s` : `${(ms / 60_000).toFixed(1)}m`;
}

export default function PipelinePage() {
  const { data: metrics, isLoading: mLoading, refetch } = useMetrics();
  const { data: quality, isLoading: qLoading }          = useQuality();

  const runs      = metrics?.recent_runs   ?? [];
  const perSource = metrics?.per_source    ?? [];
  const nlpCount  = quality?.nlp_enriched_count ?? 0;
  const total     = quality?.total              ?? 0;
  const outliers  = quality?.outlier_count      ?? 0;
  const dups      = quality?.duplicate_count    ?? 0;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-bold">Pipeline</h1>
          <Button variant="outline" size="sm" onClick={() => refetch()}
            disabled={mLoading} className="gap-2">
            {mLoading
              ? <Loader2 className="h-4 w-4 animate-spin" />
              : <RefreshCw className="h-4 w-4" />}
            Refresh
          </Button>
        </div>

        {/* Source status cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {mLoading
            ? [1,2,3].map(i => (
                <Card key={i}>
                  <CardContent className="p-5">
                    <div className="h-20 bg-muted animate-pulse rounded" />
                  </CardContent>
                </Card>
              ))
            : perSource.map((s: any) => (
                <Card key={s.source_name}>
                  <CardContent className="p-5">
                    <div className="flex items-center justify-between mb-3">
                      <h3 className="font-semibold capitalize">{s.source_name}</h3>
                      <Badge variant="outline" className="text-green-600 border-green-600">
                        Active
                      </Badge>
                    </div>
                    <div className="text-sm space-y-1">
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Total listings</span>
                        <span className="font-medium">{s.count.toLocaleString()}</span>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))}
        </div>

        {/* Enrichment + quality */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-sm">Enrichment Status</CardTitle></CardHeader>
            <CardContent className="space-y-4">
              {[
                { label: "NLP Enriched", value: nlpCount, total },
                { label: "Outliers Flagged", value: outliers, total },
                { label: "Suspected Duplicates", value: dups, total },
              ].map(row => (
                <div key={row.label}>
                  <div className="flex justify-between text-sm mb-1">
                    <span>{row.label}</span>
                    <span>{row.value.toLocaleString()} / {row.total.toLocaleString()}</span>
                  </div>
                  <div className="h-2 rounded-full bg-muted overflow-hidden">
                    <div className="h-full rounded-full bg-primary transition-all"
                      style={{ width: row.total > 0 ? `${(row.value / row.total) * 100}%` : "0%" }} />
                  </div>
                </div>
              ))}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">Latest Run Summary</CardTitle></CardHeader>
            <CardContent>
              {mLoading ? (
                <div className="h-24 bg-muted animate-pulse rounded" />
              ) : runs.length === 0 ? (
                <p className="text-sm text-muted-foreground">No runs recorded yet.</p>
              ) : (() => {
                const r = runs[0];
                return (
                  <div className="space-y-2 text-sm">
                    {[
                      ["Source",    r.source_name ?? "—"],
                      ["Started",   formatDate(r.started_at)],
                      ["Duration",  duration(r.started_at, r.finished_at)],
                      ["Fetched",   r.fetched ?? "—"],
                      ["Inserted",  r.inserted ?? "—"],
                      ["Updated",   r.updated ?? "—"],
                      ["Errors",    r.errors ?? 0],
                    ].map(([k, v]) => (
                      <div key={k} className="flex justify-between">
                        <span className="text-muted-foreground">{k}</span>
                        <span className="font-medium">{String(v)}</span>
                      </div>
                    ))}
                  </div>
                );
              })()}
            </CardContent>
          </Card>
        </div>

        {/* Recent runs log */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-sm">Recent Scraper Runs</CardTitle>
            <Button variant="outline" size="sm" disabled className="gap-2">
              <Zap className="h-4 w-4" /> Trigger Scrape
            </Button>
          </CardHeader>
          <CardContent>
            {mLoading ? (
              <div className="space-y-2">
                {[1,2,3].map(i => <div key={i} className="h-10 bg-muted animate-pulse rounded" />)}
              </div>
            ) : runs.length === 0 ? (
              <p className="text-sm text-muted-foreground">No runs yet.</p>
            ) : (
              <div className="space-y-0">
                {runs.map((r: any, i: number) => (
                  <div key={i} className="flex items-start gap-3 py-3 border-b last:border-0">
                    <CheckCircle className="h-4 w-4 text-green-500 shrink-0 mt-0.5" />
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 text-sm">
                        <Badge variant="outline" className="capitalize text-xs">
                          {r.source_name}
                        </Badge>
                        <Badge variant="secondary" className="text-xs">
                          +{r.inserted ?? 0} new
                        </Badge>
                        {r.errors > 0 && (
                          <Badge variant="destructive" className="text-xs">
                            {r.errors} errors
                          </Badge>
                        )}
                      </div>
                      <p className="text-xs text-muted-foreground mt-1">
                        {formatDate(r.started_at)} · {duration(r.started_at, r.finished_at)}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}