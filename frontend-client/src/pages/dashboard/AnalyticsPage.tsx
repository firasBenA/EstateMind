import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Loader2 } from "lucide-react";
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";
import { useEda, useQuality } from "@/hooks/useAdminData";

export default function AnalyticsPage() {
  const { data: eda,     isLoading: eLoading } = useEda();
  const { data: quality, isLoading: qLoading } = useQuality();

  const priceM2Data    = eda?.price_m2_stats      ?? [];
  const trendData      = eda?.trend_stats          ?? [];
  const topFeatures    = (eda?.top_features        ?? []).slice(0, 10);
  const nullStats      = (quality?.null_field_stats ?? []).slice(0, 8);
  const scoreDist      = quality?.score_distribution ?? [];
  const sourceQuality  = quality?.source_quality    ?? [];

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <h1 className="text-xl font-bold">Analytics</h1>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

          {/* Price/m² by region */}
          <Card>
            <CardHeader><CardTitle className="text-sm">Avg Price/m² by Region</CardTitle></CardHeader>
            <CardContent className="h-72">
              {eLoading
                ? <div className="h-full bg-muted animate-pulse rounded" />
                : (
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={priceM2Data} layout="vertical">
                      <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                      <XAxis type="number" className="text-xs"
                        tickFormatter={v => `${v.toLocaleString()}`} />
                      <YAxis type="category" dataKey="region"
                        className="text-xs" width={110} />
                      <Tooltip formatter={(v: number) => `${v.toLocaleString()} TND/m²`} />
                      <Bar dataKey="avg_m2" fill="hsl(var(--primary))" radius={[0,4,4,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                )}
            </CardContent>
          </Card>

          {/* Listings over time */}
          <Card>
            <CardHeader><CardTitle className="text-sm">Listings Over Time</CardTitle></CardHeader>
            <CardContent className="h-72">
              {eLoading
                ? <div className="h-full bg-muted animate-pulse rounded" />
                : (
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={trendData}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                      <XAxis dataKey="date" className="text-xs"
                        tickFormatter={v => v.slice(5)} />
                      <YAxis className="text-xs" />
                      <Tooltip />
                      <Line type="monotone" dataKey="count"
                        stroke="hsl(var(--primary))" strokeWidth={2} dot={false} />
                    </LineChart>
                  </ResponsiveContainer>
                )}
            </CardContent>
          </Card>

          {/* Top features */}
          <Card>
            <CardHeader><CardTitle className="text-sm">Top Listing Features</CardTitle></CardHeader>
            <CardContent className="h-72">
              {eLoading
                ? <div className="h-full bg-muted animate-pulse rounded" />
                : (
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={topFeatures} layout="vertical">
                      <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                      <XAxis type="number" className="text-xs" />
                      <YAxis type="category" dataKey="feature"
                        className="text-xs" width={130} />
                      <Tooltip />
                      <Bar dataKey="count" fill="hsl(var(--secondary))" radius={[0,4,4,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                )}
            </CardContent>
          </Card>

          {/* Reliability score distribution */}
          <Card>
            <CardHeader><CardTitle className="text-sm">Reliability Score Distribution</CardTitle></CardHeader>
            <CardContent className="h-72">
              {qLoading
                ? <div className="h-full bg-muted animate-pulse rounded" />
                : (
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={scoreDist}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                      <XAxis dataKey="level" className="text-xs" />
                      <YAxis className="text-xs" />
                      <Tooltip />
                      <Bar dataKey="count" fill="hsl(var(--primary))" radius={[4,4,0,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                )}
            </CardContent>
          </Card>
        </div>

        {/* Null field stats table */}
        <Card>
          <CardHeader><CardTitle className="text-sm">Field Completeness</CardTitle></CardHeader>
          <CardContent>
            {qLoading
              ? <div className="h-24 bg-muted animate-pulse rounded" />
              : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="pb-2 pr-6">Field</th>
                        <th className="pb-2 pr-6">Filled</th>
                        <th className="pb-2 pr-6">Missing</th>
                        <th className="pb-2">Completeness</th>
                      </tr>
                    </thead>
                    <tbody>
                      {nullStats.map((row: any) => (
                        <tr key={row.field} className="border-b last:border-0">
                          <td className="py-2 pr-6 font-medium">{row.field}</td>
                          <td className="py-2 pr-6">{row.filled_count.toLocaleString()}</td>
                          <td className="py-2 pr-6 text-muted-foreground">{row.null_count.toLocaleString()}</td>
                          <td className="py-2">
                            <div className="flex items-center gap-2">
                              <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
                                <div
                                  className="h-full bg-primary rounded-full"
                                  style={{ width: `${row.filled_pct}%` }}
                                />
                              </div>
                              <span className="text-xs w-10 text-right">{row.filled_pct}%</span>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
          </CardContent>
        </Card>

        {/* Source quality table */}
        <Card>
          <CardHeader><CardTitle className="text-sm">Quality by Source</CardTitle></CardHeader>
          <CardContent>
            {qLoading
              ? <div className="h-24 bg-muted animate-pulse rounded" />
              : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-muted-foreground">
                        <th className="pb-2 pr-6">Source</th>
                        <th className="pb-2 pr-6">Total</th>
                        <th className="pb-2 pr-6">HIGH</th>
                        <th className="pb-2 pr-6">GOOD</th>
                        <th className="pb-2 pr-6">LOW</th>
                        <th className="pb-2">DROP</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sourceQuality.map((row: any) => (
                        <tr key={row.source} className="border-b last:border-0">
                          <td className="py-2 pr-6 font-medium capitalize">{row.source}</td>
                          <td className="py-2 pr-6">{row.total.toLocaleString()}</td>
                          <td className="py-2 pr-6 text-green-600">{row.high}</td>
                          <td className="py-2 pr-6 text-blue-600">{row.good}</td>
                          <td className="py-2 pr-6 text-amber-600">{row.low}</td>
                          <td className="py-2 text-red-600">{row.drop}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}