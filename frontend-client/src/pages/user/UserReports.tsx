import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { FileText, Download, Loader2, TrendingUp, MapPin, BarChart3 } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";
import { useAuth } from "@/lib/auth-context";

const reportTemplates = [
  { id: "investment", title: "Investment Analysis Report", desc: "Comprehensive ROI, market trends, and risk analysis for your listings", icon: TrendingUp, color: "text-primary" },
  { id: "market", title: "Market Overview Report", desc: "Price trends, best areas to buy/sell/rent, regional comparisons", icon: MapPin, color: "text-chart-2" },
  { id: "portfolio", title: "Portfolio Performance", desc: "Track all your listings' performance, views, and conversion rates", icon: BarChart3, color: "text-success" },
];

const pastReports = [
  { id: "r1", title: "Investment Analysis — La Marsa Villa", date: "25 Mar 2026", type: "investment", pages: 12 },
  { id: "r2", title: "Market Overview — Q1 2026", date: "01 Mar 2026", type: "market", pages: 18 },
];

export default function UserReports() {
  const [generating, setGenerating] = useState<string | null>(null);
  const { user } = useAuth();
  const isAgency = user?.role === "agency";

  const handleGenerate = (type: string) => {
    setGenerating(type);
    setTimeout(() => {
      setGenerating(null);
      toast.success("Report generated! (RAG integration coming soon)");
    }, 2500);
  };

  return (
    <UserDashboardLayout>
      <div className="space-y-8">
        <div>
          <h1 className="text-2xl font-bold">Reports</h1>
          <p className="text-muted-foreground">
            {isAgency ? "Generate investment and market reports for your agency" : "Generate reports about your listings and investments"}
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {reportTemplates.map(t => (
            <Card key={t.id} className="hover:shadow-md transition-shadow">
              <CardHeader>
                <t.icon className={`h-8 w-8 ${t.color} mb-2`} />
                <CardTitle className="text-base">{t.title}</CardTitle>
                <CardDescription className="text-xs">{t.desc}</CardDescription>
              </CardHeader>
              <CardContent>
                <Button
                  className="w-full"
                  onClick={() => handleGenerate(t.id)}
                  disabled={generating === t.id}
                >
                  {generating === t.id ? (
                    <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Generating...</>
                  ) : (
                    <><FileText className="h-4 w-4 mr-2" /> Generate Report</>
                  )}
                </Button>
              </CardContent>
            </Card>
          ))}
        </div>

        <div>
          <h2 className="text-lg font-semibold mb-4">Past Reports</h2>
          <div className="space-y-3">
            {pastReports.map(r => (
              <Card key={r.id}>
                <CardContent className="flex items-center justify-between py-4">
                  <div className="flex items-center gap-4">
                    <FileText className="h-8 w-8 text-muted-foreground" />
                    <div>
                      <p className="font-medium text-sm">{r.title}</p>
                      <p className="text-xs text-muted-foreground">{r.date} · {r.pages} pages</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant="secondary">{r.type}</Badge>
                    <Button variant="outline" size="sm">
                      <Download className="h-4 w-4 mr-1" /> PDF
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      </div>
    </UserDashboardLayout>
  );
}
