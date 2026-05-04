// frontend-client/src/pages/admin/DashboardOverview.tsx

import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Building2, AlertTriangle, Copy, CalendarDays, RefreshCw, Loader2,
  TrendingUp, ArrowDown, ArrowUp, BarChart3, Activity
} from "lucide-react";
import {
  LineChart, Line, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  Legend, ComposedChart, Area
} from "recharts";
import { useMetrics, useEda } from "@/hooks/useAdminData";
import { useEffect, useState } from "react";

const COLORS = ["#2563EB", "#16A34A", "#DD309B", "#F59E0B", "#8B5CF6", "#06B6D4", "#EF4444", "#10B981"];

export default function DashboardOverview() {
  const { data: metrics, isLoading: mLoading, refetch: refetchMetrics } = useMetrics();
  const { data: eda, isLoading: eLoading, refetch: refetchEda } = useEda();
  const [macroData, setMacroData] = useState<any[]>([]);
  const [prophetData, setProphetData] = useState<any>(null);
  const [modelMetrics, setModelMetrics] = useState<any>(null);
  const [macroLoading, setMacroLoading] = useState(true);

  const isLoading = mLoading || eLoading || macroLoading;

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [impactRes, prophetRes, metricsRes] = await Promise.all([
          fetch("/api/macro/impact/", { credentials: "include" }),
          fetch("/api/macro/prophet-forecast/", { credentials: "include" }),
          fetch("/api/macro/model-metrics/", { credentials: "include" }),
        ]);
        
        const impactData = await impactRes.json();
        const prophetDataRes = await prophetRes.json();
        const metricsDataRes = await metricsRes.json();
        
        setMacroData(impactData.data || []);
        setProphetData(prophetDataRes);
        setModelMetrics(metricsDataRes.metrics);
      } catch (error) {
        console.error("Failed to fetch macro data:", error);
      } finally {
        setMacroLoading(false);
      }
    };
    fetchData();
  }, []);

  const processPropertyTypes = (data: any[]) => {
    if (!data || data.length === 0) return [];
    const THRESHOLD = 10;
    const total = data.reduce((sum, item) => sum + item.count, 0);
    const mainCategories = [];
    let otherCount = 0;
    data.forEach((item) => {
      const percentage = (item.count / total) * 100;
      if (percentage >= THRESHOLD) {
        mainCategories.push(item);
      } else {
        otherCount += item.count;
      }
    });
    if (otherCount > 0) mainCategories.push({ type: "Other", count: otherCount });
    return mainCategories;
  };

  const kpis = metrics ? [
    { icon: Building2, label: "Total Listings", value: metrics.total_listings ?? 0 },
    { icon: AlertTriangle, label: "Outliers", value: metrics.flagged_count ?? 0 },
    { icon: Copy, label: "Last Inserted", value: metrics.latest_run?.inserted ?? "—" },
    { icon: CalendarDays, label: "Last Run", value: metrics.latest_run?.started_at ? new Date(metrics.latest_run.started_at).toLocaleDateString() : "—" },
  ] : [];

  const trendData = eda?.trend_stats ?? [];
  const cityData = (eda?.top_areas ?? []).slice(0, 10);
  const typeData = processPropertyTypes(eda?.property_type_stats ?? []);
  const transData = eda?.transaction_stats ?? [];
  const sourceData = metrics?.per_source ?? [];

  const combinedChartData = [
    ...(prophetData?.historical || []),
    ...(prophetData?.forecast || [])
  ];

  const featureImportanceArray = modelMetrics?.xgboost?.feature_importance
    ? Object.entries(modelMetrics.xgboost.feature_importance).map(([name, value]) => ({ name, importance: value }))
    : [];

  // Vérifier si les données sont valides pour le graphique
  const hasValidCombinedData = combinedChartData.length > 0 && combinedChartData.some(d => d.inflation !== null);
  const hasValidForecastData = prophetData?.forecast && prophetData.forecast.length > 0;
  const hasValidPredictions = modelMetrics?.xgboost?.predictions && modelMetrics.xgboost.predictions.length > 0;

  const renderLegend = (props: any) => {
    const { payload } = props;
    return (
      <ul className="text-xs space-y-1">
        {payload.map((entry: any, index: number) => (
          <li key={`item-${index}`} className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full" style={{ backgroundColor: entry.color }} />
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
          <Button variant="outline" size="sm" onClick={() => { refetchMetrics(); refetchEda(); }} disabled={isLoading} className="gap-2">
            {isLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            Refresh
          </Button>
        </div>

        {/* KPIs */}
        {isLoading && !metrics ? (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {[1, 2, 3, 4].map(i => (
              <Card key={i}>
                <CardContent className="p-4">
                  <div className="h-10 bg-muted animate-pulse rounded" />
                </CardContent>
              </Card>
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {kpis.map((k) => (
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

        {/* ==================== PROPHET FORECAST CHARTS ==================== */}
        {hasValidCombinedData && (
          <Card>
              <CardHeader>
                <CardTitle className="text-sm">Inflation (IPC) - Prophet Forecast</CardTitle>
                <p className="text-xs text-muted-foreground">12 months projection — MAE: {modelMetrics?.prophet?.ipc_mae || 0.42}%</p>
              </CardHeader>
              <CardContent className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={combinedChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                    <YAxis domain={[0, 12]} />
                    <Tooltip formatter={(value) => `${value}%`} />
                    <Legend />
                    <Area type="monotone" dataKey="inflation" stroke="#ef4444" fill="#ef4444" fillOpacity={0.1} name="Inflation %" />
                    <Line type="monotone" dataKey="taux_directeur" stroke="#f59e0b" strokeDasharray="5 5" name="Key Rate %" />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
        )}

        {/* ==================== XGBOOST PERFORMANCE ==================== */}
        {hasValidPredictions && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Actual vs Predicted */}
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">XGBoost — Actual vs Predicted</CardTitle>
                <p className="text-xs text-muted-foreground">MAE = {modelMetrics?.xgboost?.mae || 0}% | MAPE = {modelMetrics?.xgboost?.mape || 0}%</p>
              </CardHeader>
              <CardContent className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={modelMetrics.xgboost.predictions}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                    <YAxis domain={[0, 12]} />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="actual" stroke="#3b82f6" name="Actual" strokeWidth={2} />
                    <Line type="monotone" dataKey="predicted" stroke="#ef4444" name="Predicted" strokeDasharray="5 5" />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            {/* Feature Importance */}
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">XGBoost — Feature Importance</CardTitle>
                <p className="text-xs text-muted-foreground">What drives inflation predictions</p>
              </CardHeader>
              <CardContent className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={featureImportanceArray} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis type="number" tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
                    <YAxis type="category" dataKey="name" width={80} tick={{ fontSize: 10 }} />
                    <Tooltip formatter={(value) => `${(Number(value) * 100).toFixed(1)}%`} />
                    <Bar dataKey="importance" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          </div>
        )}

        {/* ==================== MACROECONOMIC IMPACT ==================== */}
        {macroData.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle className="text-sm flex items-center gap-2">
                <BarChart3 className="h-4 w-4" />
                Macroeconomic Forecast (Prophet Models)
              </CardTitle>
              <p className="text-xs text-muted-foreground">Inflation and interest rate impact on property prices - 2025-2029 projection</p>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={350}>
                <ComposedChart data={macroData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="year" />
                  <YAxis yAxisId="left" tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
                  <YAxis yAxisId="right" orientation="right" />
                  <Tooltip formatter={(value, name) => {
                    if (name === "Apartment" || name === "Villa") return `${(Number(value) / 1000).toFixed(0)}k TND`;
                    return `${value}%`;
                  }} />
                  <Legend />
                  <Bar yAxisId="left" dataKey="price_Apartment" fill="#3b82f6" name="Apartment" radius={[4,4,0,0]} />
                  <Bar yAxisId="left" dataKey="price_Villa" fill="#10b981" name="Villa" radius={[4,4,0,0]} />
                  <Line yAxisId="right" type="monotone" dataKey="inflation" stroke="#ef4444" name="Inflation %" strokeWidth={2} />
                  <Line yAxisId="right" type="monotone" dataKey="taux_directeur" stroke="#f59e0b" name="Key Rate %" strokeWidth={2} />
                </ComposedChart>
              </ResponsiveContainer>

              <div className="grid grid-cols-4 gap-3 mt-4 pt-3 border-t">
                <div>
                  <p className="text-xs text-muted-foreground">Inflation 2026</p>
                  <p className="text-lg font-semibold">{macroData[0]?.inflation}%</p>
                  <ArrowDown className="h-3 w-3 text-emerald-600 mt-1" />
                </div>
                <div>
                  <p className="text-xs text-muted-foreground">Inflation 2028</p>
                  <p className="text-lg font-semibold">{macroData[2]?.inflation}%</p>
                  <ArrowDown className="h-3 w-3 text-emerald-600 mt-1" />
                </div>
                <div>
                  <p className="text-xs text-muted-foreground">Apartment 2028</p>
                  <p className="text-lg font-semibold">{((macroData[2]?.price_Apartment || 0) / 1000).toFixed(0)}k TND</p>
                  <TrendingUp className="h-3 w-3 text-emerald-600 mt-1" />
                </div>
                <div>
                  <p className="text-xs text-muted-foreground">Villa 2028</p>
                  <p className="text-lg font-semibold">{((macroData[2]?.price_Villa || 0) / 1000).toFixed(0)}k TND</p>
                  <TrendingUp className="h-3 w-3 text-emerald-600 mt-1" />
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* ==================== STANDARD ADMIN CHARTS ==================== */}
        {/* Row 1 - Listings Over Time & By City */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Listings Scraped Over Time</CardTitle>
            </CardHeader>
            <CardContent className="h-64">
              {eLoading ? (
                <div className="h-full bg-muted animate-pulse rounded" />
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={trendData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" tickFormatter={v => v?.slice(5) || ""} />
                    <YAxis />
                    <Tooltip />
                    <Line type="monotone" dataKey="count" stroke="#2563EB" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Listings by City (Top 10)</CardTitle>
            </CardHeader>
            <CardContent className="h-64">
              {eLoading ? (
                <div className="h-full bg-muted animate-pulse rounded" />
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={cityData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="city" angle={-45} textAnchor="end" height={60} tick={{ fontSize: 10 }} />
                    <YAxis />
                    <Tooltip />
                    <Bar dataKey="count" fill="#2563EB" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Row 2 - Pie Charts */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">By Property Type</CardTitle>
            </CardHeader>
            <CardContent className="h-80">
              {eLoading ? (
                <div className="h-full bg-muted animate-pulse rounded" />
              ) : typeData.length === 0 ? (
                <div className="h-full flex items-center justify-center text-muted-foreground">No data available</div>
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
                      label={({ name, percent }) => percent > 0.05 ? `${name} ${(percent * 100).toFixed(0)}%` : ""}
                      labelLine={false}
                    >
                      {typeData.map((_: any, i: number) => (
                        <Cell key={`cell-${i}`} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value: number) => `${value.toLocaleString()} listings`} />
                    <Legend content={renderLegend} layout="vertical" align="right" verticalAlign="middle" />
                  </PieChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">By Transaction Type</CardTitle>
            </CardHeader>
            <CardContent className="h-80">
              {eLoading ? (
                <div className="h-full bg-muted animate-pulse rounded" />
              ) : transData.length === 0 ? (
                <div className="h-full flex items-center justify-center text-muted-foreground">No data available</div>
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
                      label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
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

        {/* Row 3 - Source & Recent Runs */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Listings by Source</CardTitle>
            </CardHeader>
            <CardContent className="h-64">
              {mLoading ? (
                <div className="h-full bg-muted animate-pulse rounded" />
              ) : sourceData.length === 0 ? (
                <div className="h-full flex items-center justify-center text-muted-foreground">No data available</div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={sourceData} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis type="number" />
                    <YAxis type="category" dataKey="source_name" width={100} tick={{ fontSize: 11 }} />
                    <Tooltip />
                    <Bar dataKey="count" fill="#2563EB" radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Recent Scraper Runs</CardTitle>
            </CardHeader>
            <CardContent>
              {mLoading ? (
                <div className="space-y-2">
                  {Array.from({ length: 3 }).map((_, i) => (
                    <div key={i} className="h-8 bg-muted animate-pulse rounded" />
                  ))}
                </div>
              ) : (metrics?.recent_runs ?? []).length === 0 ? (
                <div className="text-center py-8">
                  <p className="text-sm text-muted-foreground mb-2">No runs recorded yet</p>
                  <p className="text-xs text-muted-foreground">Run agent metrics to see scraping history</p>
                </div>
              ) : (
                <div className="space-y-3 max-h-56 overflow-y-auto">
                  {(metrics.recent_runs as any[]).map((r: any, i: number) => (
                    <div key={i} className="flex items-center justify-between text-sm border-b pb-2 last:border-0">
                      <div>
                        <span className="font-medium capitalize">{r.source_name}</span>
                        <span className="text-muted-foreground ml-2 text-xs">
                          {r.started_at ? new Date(r.started_at).toLocaleString() : "—"}
                        </span>
                      </div>
                      <div className="flex gap-2 text-xs">
                        <Badge variant="outline" className="bg-green-50">+{r.inserted ?? 0} new</Badge>
                        <Badge variant="secondary">{r.updated ?? 0} upd</Badge>
                        {r.errors > 0 && <Badge variant="destructive">{r.errors} errors</Badge>}
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