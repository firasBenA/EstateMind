import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { mockListings, formatPricePerM2 } from "@/lib/mock-data";
import { Building2, ShieldAlert, AlertTriangle, Copy, TrendingUp, CalendarDays } from "lucide-react";
import { BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Link } from "react-router-dom";

const COLORS = ["hsl(160,80%,24%)", "hsl(37,70%,41%)", "hsl(200,70%,50%)", "hsl(280,60%,50%)", "hsl(340,65%,47%)"];

export default function DashboardOverview() {
  const flagged = mockListings.filter(l => l.fraud_flag);
  const outliers = mockListings.filter(l => l.is_outlier);
  const duplicates = mockListings.filter(l => l.suspected_duplicate);
  const today = new Date().toISOString().split("T")[0];
  const scrapedToday = mockListings.filter(l => l.scraped_at.startsWith(today));

  const kpis = [
    { icon: Building2, label: "Total Listings", value: mockListings.length },
    { icon: ShieldAlert, label: "Flagged", value: flagged.length },
    { icon: TrendingUp, label: "Avg Fraud Score", value: (mockListings.reduce((s, l) => s + l.fraud_score, 0) / mockListings.length).toFixed(2) },
    { icon: AlertTriangle, label: "Outliers", value: outliers.length },
    { icon: Copy, label: "Duplicates", value: duplicates.length },
    { icon: CalendarDays, label: "Scraped Today", value: scrapedToday.length },
  ];

  // Listings per day (last 30 days)
  const dailyData: Record<string, number> = {};
  for (let i = 29; i >= 0; i--) {
    const d = new Date(Date.now() - i * 86400000).toISOString().split("T")[0];
    dailyData[d] = 0;
  }
  mockListings.forEach(l => {
    const d = l.scraped_at.split("T")[0];
    if (d in dailyData) dailyData[d]++;
  });
  const dailyChart = Object.entries(dailyData).map(([date, count]) => ({ date: date.slice(5), count }));

  // By city (top 10)
  const cityCount: Record<string, number> = {};
  mockListings.forEach(l => { cityCount[l.city] = (cityCount[l.city] || 0) + 1; });
  const cityChart = Object.entries(cityCount).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([city, count]) => ({ city, count }));

  // By type
  const typeCount: Record<string, number> = {};
  mockListings.forEach(l => { typeCount[l.type] = (typeCount[l.type] || 0) + 1; });
  const typeChart = Object.entries(typeCount).map(([name, value]) => ({ name, value }));

  // By transaction
  const transCount: Record<string, number> = {};
  mockListings.forEach(l => { transCount[l.transaction_type] = (transCount[l.transaction_type] || 0) + 1; });
  const transChart = Object.entries(transCount).map(([name, value]) => ({ name, value }));

  const recentFlags = flagged.sort((a, b) => new Date(b.flagged_at!).getTime() - new Date(a.flagged_at!).getTime()).slice(0, 10);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* KPIs */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
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

        {/* Charts row 1 */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-sm">Listings Scraped (30 Days)</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={dailyChart}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="date" className="text-xs" />
                  <YAxis className="text-xs" />
                  <Tooltip />
                  <Line type="monotone" dataKey="count" stroke="hsl(var(--primary))" strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle className="text-sm">Listings by City (Top 10)</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={cityChart}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="city" className="text-xs" />
                  <YAxis className="text-xs" />
                  <Tooltip />
                  <Bar dataKey="count" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        {/* Charts row 2 */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-sm">By Property Type</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie data={typeChart} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>
                    {typeChart.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle className="text-sm">By Transaction Type</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie data={transChart} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>
                    {transChart.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        {/* Recent Flags */}
        <Card>
          <CardHeader><CardTitle className="text-sm">Recent Flags</CardTitle></CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left text-muted-foreground">
                    <th className="pb-2 pr-4">Title</th>
                    <th className="pb-2 pr-4">City</th>
                    <th className="pb-2 pr-4">Reason</th>
                    <th className="pb-2 pr-4">Score</th>
                    <th className="pb-2">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {recentFlags.map(l => (
                    <tr key={l.id} className="border-b last:border-0">
                      <td className="py-2 pr-4 max-w-48 truncate">{l.title}</td>
                      <td className="py-2 pr-4">{l.city}</td>
                      <td className="py-2 pr-4 text-xs">{l.fraud_reason}</td>
                      <td className="py-2 pr-4"><Badge variant="outline" className={l.fraud_score > 0.6 ? "text-destructive" : "text-warning"}>{(l.fraud_score * 100).toFixed(0)}%</Badge></td>
                      <td className="py-2"><Button asChild size="sm" variant="outline"><Link to={`/listing/${l.id}`}>Review</Link></Button></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}
