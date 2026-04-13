import { useMemo, useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { mockListings, CITIES, formatPricePerM2 } from "@/lib/mock-data";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { Badge } from "@/components/ui/badge";

const COLORS = ["hsl(160,80%,24%)", "hsl(37,70%,41%)", "hsl(200,70%,50%)"];

export default function AnalyticsPage() {
  const [cityFilter, setCityFilter] = useState("all");

  const data = useMemo(() => {
    let listings = [...mockListings];
    if (cityFilter !== "all") listings = listings.filter(l => l.city === cityFilter);

    // Avg price/m2 by month
    const monthly: Record<string, { total: number; count: number }> = {};
    listings.forEach(l => {
      const m = l.scraped_at.slice(0, 7);
      if (!monthly[m]) monthly[m] = { total: 0, count: 0 };
      monthly[m].total += l.price_per_m2;
      monthly[m].count++;
    });
    const priceOverTime = Object.entries(monthly).sort().map(([month, d]) => ({ month: month.slice(5), avg: Math.round(d.total / d.count) }));

    // Price distribution
    const ranges = [0, 50000, 100000, 200000, 500000, 1000000, 2000000, 5000000];
    const priceDistribution = ranges.slice(0, -1).map((min, i) => {
      const max = ranges[i + 1];
      return {
        range: `${(min / 1000).toFixed(0)}k-${(max / 1000).toFixed(0)}k`,
        count: listings.filter(l => l.price >= min && l.price < max).length,
      };
    });

    // By source over time
    const sourceMonthly: Record<string, Record<string, number>> = {};
    listings.forEach(l => {
      const m = l.scraped_at.slice(0, 7);
      if (!sourceMonthly[m]) sourceMonthly[m] = {};
      sourceMonthly[m][l.source_name] = (sourceMonthly[m][l.source_name] || 0) + 1;
    });
    const sourceOverTime = Object.entries(sourceMonthly).sort().map(([month, sources]) => ({ month: month.slice(5), ...sources }));

    // Reliability distribution
    const relRanges = [0, 20, 40, 60, 80, 100];
    const reliabilityDist = relRanges.slice(0, -1).map((min, i) => ({
      range: `${min}-${relRanges[i + 1]}`,
      count: listings.filter(l => l.reliability_score >= min && l.reliability_score < relRanges[i + 1]).length,
    }));

    // Fraud rate by week
    const weekly: Record<string, { total: number; flagged: number }> = {};
    listings.forEach(l => {
      const w = l.scraped_at.slice(0, 10);
      if (!weekly[w]) weekly[w] = { total: 0, flagged: 0 };
      weekly[w].total++;
      if (l.fraud_flag) weekly[w].flagged++;
    });
    const fraudRate = Object.entries(weekly).sort().slice(-20).map(([date, d]) => ({ date: date.slice(5), rate: d.total > 0 ? Math.round((d.flagged / d.total) * 100) : 0 }));

    // Top zones
    const zoneCount: Record<string, number> = {};
    listings.forEach(l => { zoneCount[l.zone] = (zoneCount[l.zone] || 0) + 1; });
    const topZones = Object.entries(zoneCount).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([zone, count]) => ({ zone, count }));

    // City overview
    const cityData: Record<string, { count: number; totalPrice: number; flagged: number }> = {};
    mockListings.forEach(l => {
      if (!cityData[l.city]) cityData[l.city] = { count: 0, totalPrice: 0, flagged: 0 };
      cityData[l.city].count++;
      cityData[l.city].totalPrice += l.price_per_m2;
      if (l.fraud_flag) cityData[l.city].flagged++;
    });
    const cityOverview = Object.entries(cityData).map(([city, d]) => ({
      city, count: d.count, avgPrice: Math.round(d.totalPrice / d.count), fraudRate: Math.round((d.flagged / d.count) * 100),
    })).sort((a, b) => b.count - a.count);

    return { priceOverTime, priceDistribution, sourceOverTime, reliabilityDist, fraudRate, topZones, cityOverview };
  }, [cityFilter]);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex gap-3">
          <Select value={cityFilter} onValueChange={setCityFilter}>
            <SelectTrigger className="w-48"><SelectValue placeholder="All Cities" /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Cities</SelectItem>
              {CITIES.map(c => <SelectItem key={c} value={c}>{c}</SelectItem>)}
            </SelectContent>
          </Select>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-sm">Avg Price/m² Over Time</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data.priceOverTime}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="month" className="text-xs" />
                  <YAxis className="text-xs" />
                  <Tooltip />
                  <Line type="monotone" dataKey="avg" stroke="hsl(var(--primary))" strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">Price Distribution</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data.priceDistribution}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="range" className="text-xs" />
                  <YAxis className="text-xs" />
                  <Tooltip />
                  <Bar dataKey="count" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">Listings by Source</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data.sourceOverTime}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="month" className="text-xs" />
                  <YAxis className="text-xs" />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="tayara" stackId="a" fill={COLORS[0]} />
                  <Bar dataKey="mubawab" stackId="a" fill={COLORS[1]} />
                  <Bar dataKey="affare" stackId="a" fill={COLORS[2]} />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">Reliability Score Distribution</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data.reliabilityDist}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="range" className="text-xs" />
                  <YAxis className="text-xs" />
                  <Tooltip />
                  <Bar dataKey="count" fill="hsl(var(--secondary))" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">Fraud Rate Over Time (%)</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data.fraudRate}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="date" className="text-xs" />
                  <YAxis className="text-xs" />
                  <Tooltip />
                  <Line type="monotone" dataKey="rate" stroke="hsl(var(--destructive))" strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">Top Zones by Listings</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data.topZones} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis type="number" className="text-xs" />
                  <YAxis type="category" dataKey="zone" className="text-xs" width={120} />
                  <Tooltip />
                  <Bar dataKey="count" fill="hsl(var(--primary))" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        {/* City Overview */}
        <Card>
          <CardHeader><CardTitle className="text-sm">City Overview</CardTitle></CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {data.cityOverview.map(c => (
                <div key={c.city} className="border rounded-lg p-4 space-y-2">
                  <h4 className="font-semibold">{c.city}</h4>
                  <div className="grid grid-cols-3 gap-2 text-center text-xs">
                    <div><div className="font-bold text-lg">{c.count}</div><span className="text-muted-foreground">Listings</span></div>
                    <div><div className="font-bold text-lg">{formatPricePerM2(c.avgPrice).replace(" TND/m²", "")}</div><span className="text-muted-foreground">TND/m²</span></div>
                    <div><div className="font-bold text-lg">{c.fraudRate}%</div><span className="text-muted-foreground">Fraud</span></div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}
