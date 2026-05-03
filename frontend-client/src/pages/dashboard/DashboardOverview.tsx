import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Building2, AlertTriangle, Copy, CalendarDays, RefreshCw, Loader2 } from "lucide-react";
import {
  LineChart, Line, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from "recharts";
import { useMetrics, useEda } from "@/hooks/useAdminData";

// Modern, professional color palette
const COLORS = [
  "#2563EB", // Blue
  "#16A34A", // Green
  "#DD309B", // Pink
  "#F59E0B", // Amber
  "#8B5CF6", // Purple
  "#06B6D4", // Cyan
  "#EF4444", // Red
  "#10B981", // Emerald
];

export default function DashboardOverview() {
  const { data: metrics, isLoading: mLoading, refetch: refetchMetrics } = useMetrics();
  const { data: eda, isLoading: eLoading, refetch: refetchEda } = useEda();

  const isLoading = mLoading || eLoading;

  // Process property type data - group small categories into "Other"
  const processPropertyTypes = (data: any[]) => {
    if (!data || data.length === 0) return [];
    
    const THRESHOLD = 10; // Percentage threshold
    const total = data.reduce((sum, item) => sum + item.count, 0);
    
    const mainCategories = [];
    let otherCount = 0;
    
    data.forEach(item => {
      const percentage = (item.count / total) * 100;
      if (percentage >= THRESHOLD) {
        mainCategories.push(item);
      } else {
        otherCount += item.count;
      }
    });
    
    if (otherCount > 0) {
      mainCategories.push({ type: "Other", count: otherCount });
    }
    
    return mainCategories;
  };

  const kpis = metrics ? [
    { icon: Building2, label: "Total Listings", value: metrics.total_listings ?? 0 },
    { icon: AlertTriangle, label: "Outliers", value: metrics.flagged_count ?? 0 },
    { icon: Copy, label: "Last Inserted", value: metrics.latest_run?.inserted ?? "—" },
    { icon: CalendarDays, label: "Last Run",
      value: metrics.latest_run?.started_at
        ? new Date(metrics.latest_run.started_at).toLocaleDateString()
        : "—" },
  ] : [];

  // Charts from EDA
  const trendData = eda?.trend_stats ?? [];
  const cityData = (eda?.top_areas ?? []).slice(0, 10);
  const rawTypeData = eda?.property_type_stats ?? [];
  const typeData = processPropertyTypes(rawTypeData);
  const transData = eda?.transaction_stats ?? [];
  const sourceData = metrics?.per_source ?? [];

  // Custom legend renderer for pie charts
  const renderLegend = (props: any) => {
    const { payload } = props;
    return (
      <ul className="text-xs space-y-1">
        {payload.map((entry: any, index: number) => (
          <li key={`item-${index}`} className="flex items-center gap-2">
            <div 
              className="w-3 h-3 rounded-full" 
              style={{ backgroundColor: entry.color }}
            />
            <span className="text-muted-foreground">{entry.value}</span>
          </li>
        ))}
      </ul>
    );
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">

        {/* Header */}
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-bold">Dashboard Overview</h1>
          <Button variant="outline" size="sm" onClick={() => { refetchMetrics(); refetchEda(); }}
            disabled={isLoading} className="gap-2">
            {isLoading
              ? <Loader2 className="h-4 w-4 animate-spin" />
              : <RefreshCw className="h-4 w-4" />}
            Refresh
          </Button>
        </div>

        {/* KPIs */}
        {isLoading ? (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {[1,2,3,4].map(i => (
              <Card key={i}>
                <CardContent className="p-4">
                  <div className="h-10 bg-muted animate-pulse rounded" />
                </CardContent>
              </Card>
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {kpis.map(k => (
              <Card key={k.label}>
                <CardContent className="p-4 flex items-center gap-3">
                  <div className="p-2 rounded-lg bg-primary/10">
                    <k.icon className="h-5 w-5 text-primary" />
                  </div>
                  <div>
                    <div className="text-2xl font-bold">{k.value}</div>
                    <div className="text-xs text-muted-foreground">{k.label}</div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}

        {/* Charts row 1 */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-sm">Listings Scraped Over Time</CardTitle></CardHeader>
            <CardContent className="h-64">
              {eLoading ? <div className="h-full bg-muted animate-pulse rounded" /> : (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={trendData}>
                    <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                    <XAxis dataKey="date" className="text-xs"
                      tickFormatter={v => v?.slice(5) || ''} />
                    <YAxis className="text-xs" />
                    <Tooltip />
                    <Line type="monotone" dataKey="count"
                      stroke="#2563EB" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">Listings by City (Top 10)</CardTitle></CardHeader>
            <CardContent className="h-64">
              {eLoading ? <div className="h-full bg-muted animate-pulse rounded" /> : (
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={cityData}>
                    <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                    <XAxis dataKey="city" className="text-xs" />
                    <YAxis className="text-xs" />
                    <Tooltip />
                    <Bar dataKey="count" fill="#2563EB" radius={[4,4,0,0]} />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Charts row 2 - Improved Pie Charts */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-sm">By Property Type</CardTitle></CardHeader>
            <CardContent className="h-80">
              {eLoading ? <div className="h-full bg-muted animate-pulse rounded" /> : typeData.length === 0 ? (
                <div className="h-full flex items-center justify-center text-muted-foreground">
                  No data available
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={typeData}
                      dataKey="count"
                      nameKey="type"
                      cx="50%"
                      cy="50%"
                      outerRadius={80}
                      label={({ name, percent }) => 
                        percent > 0.05 ? `${name} ${(percent * 100).toFixed(0)}%` : ''
                      }
                      labelLine={false}
                    >
                      {typeData.map((_: any, i: number) => (
                        <Cell key={`cell-${i}`} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value: number) => `${value.toLocaleString()} listings`} />
                    <Legend 
                      content={renderLegend}
                      layout="vertical"
                      align="right"
                      verticalAlign="middle"
                    />
                  </PieChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">By Transaction Type</CardTitle></CardHeader>
            <CardContent className="h-80">
              {eLoading ? <div className="h-full bg-muted animate-pulse rounded" /> : transData.length === 0 ? (
                <div className="h-full flex items-center justify-center text-muted-foreground">
                  No data available
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={transData}
                      dataKey="count"
                      nameKey="transaction_type"
                      cx="50%"
                      cy="50%"
                      outerRadius={80}
                      label={({ name, percent }) => 
                        `${name} ${(percent * 100).toFixed(0)}%`
                      }
                      labelLine={true}
                    >
                      {transData.map((_: any, i: number) => (
                        <Cell key={`cell-${i}`} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value: number) => `${value.toLocaleString()} listings`} />
                    <Legend content={renderLegend} />
                  </PieChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Per-source + recent runs */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-sm">Listings by Source</CardTitle></CardHeader>
            <CardContent className="h-64">
              {mLoading ? <div className="h-full bg-muted animate-pulse rounded" /> : sourceData.length === 0 ? (
                <div className="h-full flex items-center justify-center text-muted-foreground">
                  No data available
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={sourceData} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                    <XAxis type="number" className="text-xs" />
                    <YAxis type="category" dataKey="source_name"
                      className="text-xs" width={100} />
                    <Tooltip />
                    <Bar dataKey="count" fill="#2563EB" radius={[0,4,4,0]} />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">Recent Scraper Runs</CardTitle></CardHeader>
            <CardContent>
              {mLoading ? (
                <div className="space-y-2">
                  {[1,2,3].map(i => <div key={i} className="h-8 bg-muted animate-pulse rounded" />)}
                </div>
              ) : (metrics?.recent_runs ?? []).length === 0 ? (
                <div className="text-center py-8">
                  <p className="text-sm text-muted-foreground mb-2">No runs recorded yet</p>
                  <p className="text-xs text-muted-foreground">
                    Run agent metrics to see scraping history
                  </p>
                </div>
              ) : (
                <div className="space-y-3 max-h-56 overflow-y-auto">
                  {(metrics.recent_runs as any[]).map((r: any, i: number) => (
                    <div key={i} className="flex items-center justify-between text-sm border-b pb-2 last:border-0">
                      <div>
                        <span className="font-medium capitalize">{r.source_name}</span>
                        <span className="text-muted-foreground ml-2 text-xs">
                          {r.started_at
                            ? new Date(r.started_at).toLocaleString()
                            : "—"}
                        </span>
                      </div>
                      <div className="flex gap-2 text-xs">
                        <Badge variant="outline" className="bg-green-50">+{r.inserted ?? 0} new</Badge>
                        <Badge variant="secondary">{r.updated ?? 0} upd</Badge>
                        {r.errors > 0 && (
                          <Badge variant="destructive">{r.errors} errors</Badge>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </DashboardLayout>
  );
}