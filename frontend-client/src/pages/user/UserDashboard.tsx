// frontend-client/src/pages/user/UserDashboard.tsx

import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent } from "@/components/ui/card";
import { useAuth } from "@/lib/auth-context";
import { useEffect, useState } from "react";
import { listingsApi } from "@/lib/api";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ComposedChart,
  Bar,
  Area,
  Legend,
} from "recharts";
import {
  TrendingDown,
  TrendingUp,
  Activity,
  BarChart3,
  FileChartColumn
} from "lucide-react";

export default function UserDashboard() {
  const { user } = useAuth();
  const [stats, setStats] = useState<any>(null);
  const [activities, setActivities] = useState<any[]>([]);
  const [macroData, setMacroData] = useState<any[]>([]);
  const [macroSummary, setMacroSummary] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const [statsData, activityData, macroImpact, macroSummaryData] =
          await Promise.all([
            listingsApi.getUserStats(),
            listingsApi.getUserActivity(),
            fetch("/api/macro/impact/", { credentials: "include" }).then((r) =>
              r.json(),
            ),
            fetch("/api/macro/summary/", { credentials: "include" }).then((r) =>
              r.json(),
            ),
          ]);
        setStats(statsData);
        setActivities(activityData.activities);
        setMacroData(macroImpact.data || []);
        setMacroSummary(macroSummaryData.data);
      } catch (error) {
        console.error("Failed to load dashboard data:", error);
      } finally {
        setLoading(false);
      }
    };
    loadData();
  }, []);

  if (loading) {
    return (
      <UserDashboardLayout>
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
        </div>
      </UserDashboardLayout>
    );
  }

  return (
    <UserDashboardLayout>
      <div className="space-y-8 max-w-7xl mx-auto px-6">
        {/* Welcome */}
        <div>
          <h1 className="text-2xl font-semibold">
            Bonjour, {user?.name || "User"}
          </h1>
          <p className="text-muted-foreground text-sm">
            Voici l'aperçu de votre activité et des tendances du marché
          </p>
        </div>

        {/* Stats simples */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="border rounded-lg p-4">
            <p className="text-2xl font-bold">{stats?.active_listings || 0}</p>
            <p className="text-xs text-muted-foreground">Annonces actives</p>
            <span className="text-xs text-emerald-600">
              +{stats?.active_change?.split(" ")[0] || 0}
            </span>
          </div>
          <div className="border rounded-lg p-4">
            <p className="text-2xl font-bold">
              {stats?.total_views?.toLocaleString() || 0}
            </p>
            <p className="text-xs text-muted-foreground">Vues totales</p>
            <span className="text-xs text-emerald-600">
              {stats?.views_change || "0%"}
            </span>
          </div>
          <div className="border rounded-lg p-4">
            <p className="text-2xl font-bold">{stats?.total_likes || 0}</p>
            <p className="text-xs text-muted-foreground">Favoris reçus</p>
            <span className="text-xs text-muted-foreground">
              {stats?.likes_change || "0 cette semaine"}
            </span>
          </div>
          <div className="border rounded-lg p-4">
            <p className="text-2xl font-bold">+{stats?.roi_estimate || 0}%</p>
            <p className="text-xs text-muted-foreground">ROI estimé</p>
            <span className="text-xs text-muted-foreground">
              Moyenne portefeuille
            </span>
          </div>
        </div>

        {/* Market Summary Cards - Version élégante */}
        {macroSummary && (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-gradient-to-br from-slate-50 to-white dark:from-slate-950 dark:to-slate-900 rounded-lg border p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-muted-foreground">
                    	Current Inflation
                  </p>
                  <p className="text-3xl font-bold tracking-tight">
                    {macroSummary.current_inflation}%
                  </p>
                </div>
                <div className="h-10 w-10 rounded-full bg-amber-100 dark:bg-amber-950/30 flex items-center justify-center">
                  <TrendingDown className="h-5 w-5 text-amber-600" />
                </div>
              </div>
              <div className="mt-3">
                <div className="flex items-center gap-2 text-sm">
                  <span className="text-emerald-600">▼</span>
                  <span className="text-muted-foreground">
                    Forecast at {macroSummary.next_year_inflation}% en 2026
                  </span>
                </div>
              </div>
            </div>

            <div className="bg-gradient-to-br from-slate-50 to-white dark:from-slate-950 dark:to-slate-900 rounded-lg border p-5">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-muted-foreground">
                    Key Rate
                  </p>
                  <p className="text-3xl font-bold tracking-tight">
                    {macroSummary.current_taux}%
                  </p>
                </div>
                <div className="h-10 w-10 rounded-full bg-blue-100 dark:bg-blue-950/30 flex items-center justify-center">
                  <Activity className="h-5 w-5 text-blue-600" />
                </div>
              </div>
              <div className="mt-3">
                <p className="text-sm text-muted-foreground">
                  Central Bank Rate
                </p>
              </div>
            </div>

            <div
              className={`bg-gradient-to-br from-slate-50 to-white dark:from-slate-950 dark:to-slate-900 rounded-lg border p-5 ${
                macroSummary.trend === "down"
                  ? "border-emerald-200 dark:border-emerald-800"
                  : "border-amber-200"
              }`}
            >
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-muted-foreground">
                    Market Trend
                  </p>
                  <p
                    className={`text-3xl font-bold tracking-tight ${
                      macroSummary.trend === "down"
                        ? "text-emerald-600"
                        : "text-amber-600"
                    }`}
                  >
                    
                    
                    {macroSummary.trend === "down" ? "Down" : "Up"}
                  </p>
                </div>
                <div
                  className={`h-10 w-10 rounded-full flex items-center justify-center ${
                    macroSummary.trend === "down"
                      ? "bg-emerald-100 dark:bg-emerald-950/30"
                      : "bg-amber-100"
                  }`}
                >
                  {macroSummary.trend === "down" ? (
                    <TrendingDown className="h-5 w-5 text-emerald-600" />
                  ) : (
                    <TrendingUp className="h-5 w-5 text-amber-600" />
                  )}
                </div>
              </div>
              <div className="mt-3">
                <p className="text-sm text-muted-foreground">
                  L'inflation diminue → pouvoir d'achat s'améliore
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Impact Chart - Version propre */}
        {macroData.length > 0 && (
          <div className="rounded-lg border bg-white dark:bg-slate-950 p-5">
            <div className="mb-4">
              <h3 className="font-semibold">
                Impact de l'Inflation sur les Prix Immobiliers
              </h3>
              <p className="text-xs text-muted-foreground mt-1">
                Projection basée sur les modèles Prophet (inflation + taux
                directeur)
              </p>
            </div>

            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={macroData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis
                    dataKey="year"
                    tick={{ fontSize: 11 }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    yAxisId="left"
                    tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`}
                    tick={{ fontSize: 11 }}
                    axisLine={false}
                    tickLine={false}
                    width={50}
                  />
                  <YAxis
                    yAxisId="right"
                    orientation="right"
                    tick={{ fontSize: 11 }}
                    axisLine={false}
                    tickLine={false}
                    width={35}
                  />
                  <Tooltip
                    formatter={(value: number, name: string) => {
                      if (name === "Appartement" || name === "Villa") {
                        return [`${(value / 1000).toFixed(0)}k TND`, name];
                      }
                      return [`${value}%`, name];
                    }}
                    contentStyle={{ fontSize: 12, borderRadius: 8 }}
                  />
                  <Legend
                    wrapperStyle={{ fontSize: 11, paddingTop: 16 }}
                    iconType="circle"
                  />
                  <Area
                    yAxisId="left"
                    type="monotone"
                    dataKey="price_Apartment"
                    name="Appartement"
                    stroke="#3b82f6"
                    fill="#3b82f6"
                    fillOpacity={0.08}
                    strokeWidth={2}
                  />
                  <Area
                    yAxisId="left"
                    type="monotone"
                    dataKey="price_Villa"
                    name="Villa"
                    stroke="#10b981"
                    fill="#10b981"
                    fillOpacity={0.08}
                    strokeWidth={2}
                  />
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="inflation"
                    name="Inflation"
                    stroke="#ef4444"
                    strokeWidth={2}
                    dot={{ r: 3, strokeWidth: 1 }}
                  />
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="taux_directeur"
                    name="Taux directeur"
                    stroke="#f59e0b"
                    strokeWidth={2}
                    dot={{ r: 3, strokeWidth: 1 }}
                  />
                </ComposedChart>
              </ResponsiveContainer>
            </div>

            <div className="mt-5 p-3 bg-slate-50 dark:bg-slate-900 rounded-lg">
              <p className="text-xs text-muted-foreground leading-relaxed">
                <strong>Interprétation :</strong> L'inflation élevée (ligne
                rouge) pousse les prix immobiliers à la hausse, mais un taux
                directeur élevé (ligne orange) freine l'accessibilité.
                Actuellement, l'inflation tend à baisser, ce qui pourrait
                stabiliser les prix.
              </p>
            </div>
          </div>
        )}

        {/* Recent Activity - Simple */}
        <div className="rounded-lg border p-5">
          <h3 className="font-semibold mb-3">Activité récente</h3>
          <div className="space-y-2">
            {activities.length > 0 ? (
              activities.slice(0, 5).map((item, i) => (
                <div
                  key={i}
                  className="flex justify-between items-center py-2 border-b last:border-0 text-sm"
                >
                  <span>{item.text}</span>
                  <span className="text-xs text-muted-foreground">
                    {item.time}
                  </span>
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                Aucune activité récente
              </p>
            )}
          </div>
        </div>
      </div>
    </UserDashboardLayout>
  );
}
