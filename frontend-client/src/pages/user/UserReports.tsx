/**
 * frontend-client/src/pages/user/UserReports.tsx
 *
 * Report generation page with dynamic date selection.
 * - Param forms per report type
 * - SSE streaming from /api/reports/generate/
 * - Token-by-token display as LLM writes
 * - Save report to backend
 * - Copy to clipboard / basic PDF export
 * - SELECT MONTH/YEAR for reports
 */
import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Button }   from "@/components/ui/button";
import { Input }    from "@/components/ui/input";
import { Label }    from "@/components/ui/label";
import { Badge }    from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { toast }    from "sonner";
import { useAuth }  from "@/lib/auth-context";
import { useListingsMeta } from "@/hooks/useListings";
import {
  FileText, Download, Loader2, TrendingUp, MapPin,
  BarChart3, Copy, Save, ChevronRight, Sparkles, AlertCircle,
  Eye, Edit3, Calendar
} from "lucide-react";

// ── Types ─────────────────────────────────────────────────────────────────────

type ReportType = "market" | "investment";
type PeriodType = "latest" | "monthly" | "quarterly" | "annual" | "ytd";

interface PeriodConfig {
  type: PeriodType;
  year?: number;
  month?: number;
  quarter?: number;
}

interface MarketParams {
  city:             string;
  transaction_type: string;
  period:           PeriodConfig;
}

interface InvestmentParams {
  city:             string;
  property_type:    string;
  transaction_type: string;
  budget_min:       string;
  budget_max:       string;
  period:           PeriodConfig;
}

interface SavedReport {
  id:          number;
  report_type: string;
  title:       string;
  created_at:  string;
}

// ── Config ────────────────────────────────────────────────────────────────────

const REPORT_TYPES = [
  {
    id:    "market" as ReportType,
    title: "Market Overview Report",
    desc:  "Price trends, supply/demand, regional comparisons — grounded in live listings and Tunisia market data.",
    icon:  MapPin,
    color: "text-[hsl(200_70%_50%)]",
    badge: "RAG · Live Data",
  },
  {
    id:    "investment" as ReportType,
    title: "Investment Analysis Report",
    desc:  "ROI estimate, rental yield, risk assessment and recommendations for your target segment.",
    icon:  TrendingUp,
    color: "text-primary",
    badge: "RAG · AI Analysis",
  },
] as const;

const PROP_TYPES = ["Apartment", "Villa", "Land", "Commercial"];
const PROP_LABELS: Record<string, string> = {
  Apartment: "Apartment",
  Villa: "Villa",
  Land: "Land",
  Commercial: "Commercial",
};

// ── Helper to build period payload ────────────────────────────────────────────

function getPeriodPayload(period: PeriodConfig): { start_date: string; end_date: string; report_type: string } {
  const now = new Date();
  const year = period.year || 2026;
  
  if (period.type === "latest") {
    return { start_date: "2026-03-01", end_date: "2026-03-31", report_type: "monthly" };
  }
  
  if (period.type === "monthly" && period.month) {
    const month = period.month;
    const lastDay = new Date(year, month, 0).getDate();
    return {
      start_date: `${year}-${String(month).padStart(2, "0")}-01`,
      end_date: `${year}-${String(month).padStart(2, "0")}-${lastDay}`,
      report_type: "monthly"
    };
  }
  
  if (period.type === "quarterly" && period.quarter) {
    const startMonth = (period.quarter - 1) * 3 + 1;
    const endMonth = period.quarter * 3;
    const lastDay = new Date(year, endMonth, 0).getDate();
    return {
      start_date: `${year}-${String(startMonth).padStart(2, "0")}-01`,
      end_date: `${year}-${String(endMonth).padStart(2, "0")}-${lastDay}`,
      report_type: "quarterly"
    };
  }
  
  if (period.type === "annual") {
    return {
      start_date: `${year}-01-01`,
      end_date: `${year}-12-31`,
      report_type: "annual"
    };
  }
  
  if (period.type === "ytd") {
    return {
      start_date: `${year}-01-01`,
      end_date: `${year}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`,
      report_type: "ytd"
    };
  }
  
  return { start_date: "2026-03-01", end_date: "2026-03-31", report_type: "monthly" };
}

function formatPeriodLabel(period: PeriodConfig): string {
  if (period.type === "latest") return "Latest month (March 2026)";
  if (period.type === "monthly" && period.month) {
    const months = ["January", "February", "March", "April", "May", "June", 
                    "July", "August", "September", "October", "November", "December"];
    return `${months[period.month - 1]} ${period.year}`;
  }
  if (period.type === "quarterly" && period.quarter) {
    return `Q${period.quarter} ${period.year}`;
  }
  if (period.type === "annual") return `Year ${period.year}`;
  if (period.type === "ytd") return `Year-to-Date ${period.year}`;
  return "Custom period";
}

// ── Period selector component ─────────────────────────────────────────────────

function PeriodSelector({ value, onChange }: { value: PeriodConfig; onChange: (p: PeriodConfig) => void }) {
  const currentYear = new Date().getFullYear();
  
  return (
    <div className="space-y-3">
      <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider flex items-center gap-2">
        <Calendar className="h-3.5 w-3.5" />
        Analysis Period
      </Label>
      <Tabs value={value.type} onValueChange={(v) => onChange({ type: v as PeriodType, year: value.year })}>
        <TabsList className="grid w-full grid-cols-5">
          <TabsTrigger value="latest" className="text-xs">Latest</TabsTrigger>
          <TabsTrigger value="monthly" className="text-xs">Monthly</TabsTrigger>
          <TabsTrigger value="quarterly" className="text-xs">Quarterly</TabsTrigger>
          <TabsTrigger value="annual" className="text-xs">Annual</TabsTrigger>
          <TabsTrigger value="ytd" className="text-xs">YTD</TabsTrigger>
        </TabsList>
        
        <TabsContent value="monthly" className="mt-3">
          <div className="flex gap-3">
            <Select value={String(value.year || currentYear)} onValueChange={(v) => onChange({ ...value, year: parseInt(v) })}>
              <SelectTrigger className="w-28">
                <SelectValue placeholder="Year" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="2025">2025</SelectItem>
                <SelectItem value="2026">2026</SelectItem>
              </SelectContent>
            </Select>
            <Select value={String(value.month || 3)} onValueChange={(v) => onChange({ ...value, month: parseInt(v) })}>
              <SelectTrigger className="w-36">
                <SelectValue placeholder="Month" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="1">January</SelectItem>
                <SelectItem value="2">February</SelectItem>
                <SelectItem value="3">March</SelectItem>
                <SelectItem value="4">April</SelectItem>
                <SelectItem value="5">May</SelectItem>
                <SelectItem value="6">June</SelectItem>
                <SelectItem value="7">July</SelectItem>
                <SelectItem value="8">August</SelectItem>
                <SelectItem value="9">September</SelectItem>
                <SelectItem value="10">October</SelectItem>
                <SelectItem value="11">November</SelectItem>
                <SelectItem value="12">December</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </TabsContent>
        
        <TabsContent value="quarterly" className="mt-3">
          <div className="flex gap-3">
            <Select value={String(value.year || currentYear)} onValueChange={(v) => onChange({ ...value, year: parseInt(v) })}>
              <SelectTrigger className="w-28">
                <SelectValue placeholder="Year" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="2025">2025</SelectItem>
                <SelectItem value="2026">2026</SelectItem>
              </SelectContent>
            </Select>
            <Select value={String(value.quarter || 1)} onValueChange={(v) => onChange({ ...value, quarter: parseInt(v) })}>
              <SelectTrigger className="w-32">
                <SelectValue placeholder="Quarter" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="1">Q1 (Jan-Mar)</SelectItem>
                <SelectItem value="2">Q2 (Apr-Jun)</SelectItem>
                <SelectItem value="3">Q3 (Jul-Sep)</SelectItem>
                <SelectItem value="4">Q4 (Oct-Dec)</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </TabsContent>
        
        <TabsContent value="annual" className="mt-3">
          <Select value={String(value.year || currentYear)} onValueChange={(v) => onChange({ ...value, year: parseInt(v) })}>
            <SelectTrigger className="w-32">
              <SelectValue placeholder="Year" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="2025">Year 2025</SelectItem>
              <SelectItem value="2026">Year 2026</SelectItem>
            </SelectContent>
          </Select>
        </TabsContent>
        
        <TabsContent value="ytd" className="mt-3">
          <Select value={String(value.year || currentYear)} onValueChange={(v) => onChange({ ...value, year: parseInt(v) })}>
            <SelectTrigger className="w-32">
              <SelectValue placeholder="Year" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="2025">2025 (YTD)</SelectItem>
              <SelectItem value="2026">2026 (YTD)</SelectItem>
            </SelectContent>
          </Select>
          <p className="text-xs text-muted-foreground mt-2">
            From January 1st to current date
          </p>
        </TabsContent>
        
        <TabsContent value="latest" className="mt-3">
          <p className="text-xs text-muted-foreground bg-muted/30 p-2 rounded">
            Analysis based on most recent available data (March 2026)
          </p>
        </TabsContent>
      </Tabs>
    </div>
  );
}

// ── Simple markdown renderer ─────────────────────────────────────────────────

function MarkdownBlock({ text }: { text: string }) {
  const lines = text.split("\n");
  return (
    <div className="space-y-1.5 font-mono text-[13px] leading-relaxed">
      {lines.map((line, i) => {
        if (line.startsWith("## ")) {
          return <p key={i} className="text-base font-bold text-foreground mt-4 mb-1">{line.slice(3)}</p>;
        }
        if (line.startsWith("# ")) {
          return <p key={i} className="text-lg font-bold text-foreground mt-5 mb-2">{line.slice(2)}</p>;
        }
        if (line.startsWith("### ")) {
          return <p key={i} className="text-sm font-semibold text-foreground mt-3">{line.slice(4)}</p>;
        }
        if (line.startsWith("- ") || line.startsWith("* ")) {
          return <p key={i} className="text-muted-foreground pl-4 before:content-['•'] before:mr-2">{line.slice(2)}</p>;
        }
        if (line.trim() === "") return <div key={i} className="h-2" />;
        const parts = line.split(/(\*\*[^*]+\*\*)/g);
        return (
          <p key={i} className="text-muted-foreground">
            {parts.map((part, j) =>
              part.startsWith("**") && part.endsWith("**")
                ? <strong key={j} className="text-foreground font-semibold">{part.slice(2, -2)}</strong>
                : part
            )}
          </p>
        );
      })}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function UserReports() {
  const { user } = useAuth();
  const { meta } = useListingsMeta();
  
  // ✅ CORRECTION: Remove duplicate cities using Set
  const uniqueCities = useMemo(() => {
    if (!meta?.cities) return [];
    return [...new Set(meta.cities)];
  }, [meta?.cities]);

  const [selectedType, setSelectedType] = useState<ReportType | null>(null);

  // Params with period
  const [marketParams, setMarketParams] = useState<MarketParams>({ 
    city: "", 
    transaction_type: "",
    period: { type: "latest", year: 2026 }
  });
  
  const [investmentParams, setInvestmentParams] = useState<InvestmentParams>({
    city: "", 
    property_type: "", 
    transaction_type: "sale", 
    budget_min: "", 
    budget_max: "",
    period: { type: "latest", year: 2026 }
  });

  // Generation state
  const [generating, setGenerating] = useState(false);
  const [reportText, setReportText] = useState("");
  const [streamError, setStreamError] = useState("");
  const [saving, setSaving]         = useState(false);
  const [savedId, setSavedId]       = useState<number | null>(null);
  const [isEditing, setIsEditing]   = useState(false);

  // Past reports
  const [pastReports, setPastReports] = useState<SavedReport[]>([]);

  const reportRef = useRef<HTMLDivElement>(null);

  // Fetch past reports on mount
  useEffect(() => {
    fetch("/api/reports/", { credentials: "include" })
      .then(r => r.ok ? r.json() : { reports: [] })
      .then(d => setPastReports(d.reports ?? []))
      .catch(() => {});
  }, [savedId]);

  // Build title with period
  const buildTitle = useCallback(() => {
    if (!selectedType) return "Report";
    const periodLabel = selectedType === "market" 
      ? formatPeriodLabel(marketParams.period)
      : formatPeriodLabel(investmentParams.period);
    const city = selectedType === "market" ? marketParams.city : investmentParams.city;
    const cityPart = city ? ` — ${city}` : "";
    const typeLabel = selectedType === "market" ? "Market Overview" : "Investment Analysis";
    return `${typeLabel}${cityPart} · ${periodLabel}`;
  }, [selectedType, marketParams, investmentParams]);

  // ── Generate ───────────────────────────────────────────────────────────────
  const handleGenerate = useCallback(async () => {
    if (!selectedType) return;

    setGenerating(true);
    setReportText("");
    setStreamError("");
    setSavedId(null);

    let params;
    
    if (selectedType === "market") {
      const periodPayload = getPeriodPayload(marketParams.period);
      params = {
        city: marketParams.city,
        transaction_type: marketParams.transaction_type,
        period: periodPayload
      };
    } else {
      const periodPayload = getPeriodPayload(investmentParams.period);
      params = {
        city: investmentParams.city,
        property_type: investmentParams.property_type,
        transaction_type: investmentParams.transaction_type,
        budget_min: Number(investmentParams.budget_min) || 0,
        budget_max: Number(investmentParams.budget_max) || 5_000_000,
        period: periodPayload
      };
    }

    try {
      const resp = await fetch("/api/reports/generate/", {
        method:      "POST",
        credentials: "include",
        headers:     { "Content-Type": "application/json" },
        body:        JSON.stringify({ type: selectedType, params }),
      });

      if (!resp.ok || !resp.body) {
        throw new Error(`HTTP ${resp.status}`);
      }

      const reader  = resp.body.getReader();
      const decoder = new TextDecoder();
      let   buffer  = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n\n");
        buffer = lines.pop() ?? "";

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const evt = JSON.parse(line.slice(6));
            if (evt.error) {
              setStreamError(evt.error);
              break;
            }
            if (evt.token) {
              setReportText(prev => prev + evt.token);
            }
          } catch { /* malformed line */ }
        }
      }
    } catch (e) {
      setStreamError(e instanceof Error ? e.message : "Generation failed");
    } finally {
      setGenerating(false);
      setTimeout(() => reportRef.current?.scrollIntoView({ behavior: "smooth" }), 100);
    }
  }, [selectedType, marketParams, investmentParams]);

  // ── Save ───────────────────────────────────────────────────────────────────
  const handleSave = async () => {
    if (!reportText || !selectedType) return;
    setSaving(true);
    
    const title = buildTitle();
    const params = selectedType === "market"
      ? { ...marketParams, period: getPeriodPayload(marketParams.period) }
      : { ...investmentParams, budget_min: Number(investmentParams.budget_min) || 0, 
          budget_max: Number(investmentParams.budget_max) || 5_000_000,
          period: getPeriodPayload(investmentParams.period) };
    
    try {
      const resp = await fetch("/api/reports/save/", {
        method:      "POST",
        credentials: "include",
        headers:     { "Content-Type": "application/json" },
        body:        JSON.stringify({ type: selectedType, title, params, content: reportText }),
      });
      const data = await resp.json();
      if (resp.ok) {
        setSavedId(data.id);
        toast.success("Report saved successfully");
      } else {
        toast.error(data.error ?? "Failed to save");
      }
    } catch {
      toast.error("Network error");
    } finally {
      setSaving(false);
    }
  };

  // ── Export ─────────────────────────────────────────────────────────────────
  const handleCopy = () => {
    navigator.clipboard.writeText(reportText);
    toast.success("Copied to clipboard");
  };

  const handleExportPDF = async () => {
    if (!savedId) {
      toast.error("Please save the report before exporting as PDF");
      return;
    }
    window.open(`/api/reports/${savedId}/pdf/`, "_blank");
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <UserDashboardLayout>
      <div className="space-y-8 max-w-8xl">

        {/* Header */}
        <div>
          <h1 className="text-2xl font-bold">AI Reports</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Generate data-grounded real estate reports using live market data.
            Select a time period to analyse specific months or years.
          </p>
        </div>

        {/* Step 1 — Pick type */}
        <div>
          <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground mb-3">
            Step 1 — Choose report type
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {REPORT_TYPES.map(t => (
              <button
                key={t.id}
                onClick={() => { setSelectedType(t.id); setReportText(""); setStreamError(""); }}
                className={`text-left rounded-xl border p-4 transition-all
                  ${selectedType === t.id
                    ? "border-primary bg-primary/5 shadow-sm"
                    : "hover:border-border/80 hover:bg-muted/30 bg-card"
                  }`}
              >
                <div className="flex items-start justify-between mb-2">
                  <t.icon className={`h-5 w-5 ${t.color}`} />
                  <Badge variant="secondary" className="text-[10px]">{t.badge}</Badge>
                </div>
                <p className="font-semibold text-sm">{t.title}</p>
                <p className="text-xs text-muted-foreground mt-1 leading-snug">{t.desc}</p>
              </button>
            ))}
          </div>
        </div>

        {/* Step 2 — Params */}
        {selectedType && (
          <div>
            <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground mb-3">
              Step 2 — Configure parameters
            </p>
            <Card>
              <CardContent className="pt-5 space-y-5">
                {selectedType === "market" && (
                  <div className="space-y-4">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="space-y-1.5">
                        <Label>City <span className="text-muted-foreground text-xs">(optional)</span></Label>
                        <Select value={marketParams.city || "all"} onValueChange={v => setMarketParams(p => ({ ...p, city: v === "all" ? "" : v }))}>
                          <SelectTrigger><SelectValue placeholder="All Tunisia" /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All Tunisia</SelectItem>
                            {uniqueCities.map(c => (
                              <SelectItem key={c} value={c}>{c}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-1.5">
                        <Label>Transaction type</Label>
                        <Select value={marketParams.transaction_type || "all"} onValueChange={v => setMarketParams(p => ({ ...p, transaction_type: v === "all" ? "" : v }))}>
                          <SelectTrigger><SelectValue placeholder="Sale & Rent" /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">Sale & Rent</SelectItem>
                            <SelectItem value="sale">Sale only</SelectItem>
                            <SelectItem value="rent">Rent only</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                    <PeriodSelector 
                      value={marketParams.period} 
                      onChange={(period) => setMarketParams(p => ({ ...p, period }))} 
                    />
                  </div>
                )}

                {selectedType === "investment" && (
                  <div className="space-y-4">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="space-y-1.5">
                        <Label>City</Label>
                        <Select value={investmentParams.city || "all"} onValueChange={v => setInvestmentParams(p => ({ ...p, city: v === "all" ? "" : v }))}>
                          <SelectTrigger><SelectValue placeholder="All Tunisia" /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All Tunisia</SelectItem>
                            {uniqueCities.map(c => (
                              <SelectItem key={c} value={c}>{c}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-1.5">
                        <Label>Property type</Label>
                        <Select value={investmentParams.property_type || "all"} onValueChange={v => setInvestmentParams(p => ({ ...p, property_type: v === "all" ? "" : v }))}>
                          <SelectTrigger><SelectValue placeholder="All types" /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All types</SelectItem>
                            {PROP_TYPES.map(t => <SelectItem key={t} value={t}>{PROP_LABELS[t]}</SelectItem>)}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-1.5">
                        <Label>Transaction</Label>
                        <Select value={investmentParams.transaction_type} onValueChange={v => setInvestmentParams(p => ({ ...p, transaction_type: v }))}>
                          <SelectTrigger><SelectValue /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="sale">Sale</SelectItem>
                            <SelectItem value="rent">Rent</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-1.5">
                        <Label>Budget range (TND)</Label>
                        <div className="flex gap-2 items-center">
                          <Input
                            type="number" placeholder="Min"
                            value={investmentParams.budget_min}
                            onChange={e => setInvestmentParams(p => ({ ...p, budget_min: e.target.value }))}
                          />
                          <span className="text-muted-foreground text-sm shrink-0">–</span>
                          <Input
                            type="number" placeholder="Max"
                            value={investmentParams.budget_max}
                            onChange={e => setInvestmentParams(p => ({ ...p, budget_max: e.target.value }))}
                          />
                        </div>
                      </div>
                    </div>
                    <PeriodSelector 
                      value={investmentParams.period} 
                      onChange={(period) => setInvestmentParams(p => ({ ...p, period }))} 
                    />
                  </div>
                )}

                <Button
                  onClick={handleGenerate}
                  disabled={generating}
                  className="w-full gap-2 mt-2"
                  size="lg"
                >
                  {generating ? (
                    <><Loader2 className="h-4 w-4 animate-spin" /> Generating report...</>
                  ) : (
                    <><Sparkles className="h-4 w-4" /> Generate Report</>
                  )}
                </Button>

                {generating && (
                  <p className="text-xs text-center text-muted-foreground">
                    The local AI is writing your report — this takes 30–90 seconds.
                  </p>
                )}
              </CardContent>
            </Card>
          </div>
        )}

        {/* Step 3 — Report output */}
        {(generating || reportText || streamError) && (
          <div ref={reportRef}>
            <div className="flex items-center justify-between mb-3">
              <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                Step 3 — Generated report
              </p>
              {reportText && !generating && (
                <div className="flex gap-2">
                  <Button
                    variant={isEditing ? "default" : "outline"}
                    size="sm"
                    onClick={() => setIsEditing(!isEditing)}
                    className="gap-1.5"
                  >
                    {isEditing ? <Eye className="h-3.5 w-3.5" /> : <Edit3 className="h-3.5 w-3.5" />}
                    {isEditing ? "Preview" : "Modify"}
                  </Button>
                  <Button variant="outline" size="sm" onClick={handleCopy} className="gap-1.5">
                    <Copy className="h-3.5 w-3.5" /> Copy
                  </Button>
                  <Button variant="outline" size="sm" onClick={handleExportPDF} className="gap-1.5">
                    <Download className="h-3.5 w-3.5" /> Export PDF
                  </Button>
                  <Button
                    size="sm" onClick={handleSave}
                    disabled={saving || (!!savedId && !isEditing)}
                    className="gap-1.5"
                  >
                    <Save className="h-3.5 w-3.5" />
                    {savedId && !isEditing ? "Saved ✓" : saving ? "Saving…" : "Save Changes"}
                  </Button>
                </div>
              )}
            </div>

            <Card>
              <CardContent className="pt-5">
                {streamError && (
                  <div className="flex items-start gap-2 text-destructive text-sm">
                    <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
                    <div>
                      <p className="font-medium">Generation failed</p>
                      <p className="text-xs mt-0.5 text-muted-foreground">{streamError}</p>
                      <p className="text-xs mt-1 text-muted-foreground">
                        Make sure Ollama is running and gemma3:4b is pulled:
                        <code className="bg-muted px-1 rounded ml-1">ollama pull gemma3:4b</code>
                      </p>
                    </div>
                  </div>
                )}

                {!streamError && (
                  <div className="min-h-[200px]">
                    {generating && !reportText && (
                      <div className="space-y-3">
                        <Skeleton className="h-4 w-1/3" />
                        <Skeleton className="h-3 w-full" />
                        <Skeleton className="h-3 w-5/6" />
                        <Skeleton className="h-3 w-4/5" />
                        <Skeleton className="h-4 w-1/4 mt-4" />
                        <Skeleton className="h-3 w-full" />
                        <Skeleton className="h-3 w-3/4" />
                      </div>
                    )}

                    {reportText && (
                      <>
                        {isEditing ? (
                          <textarea
                            value={reportText}
                            onChange={(e) => {
                              setReportText(e.target.value);
                              if (savedId) setSavedId(null);
                            }}
                            className="w-full min-h-[500px] p-4 font-mono text-sm border rounded-lg bg-muted/20 focus:outline-none focus:ring-1 focus:ring-primary leading-relaxed"
                            placeholder="Edit your report here..."
                          />
                        ) : (
                          <MarkdownBlock text={reportText} />
                        )}
                        {generating && !isEditing && (
                          <span className="inline-block w-1.5 h-4 bg-primary animate-pulse ml-0.5 align-middle" />
                        )}
                      </>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        )}

        {/* Past reports */}
        {pastReports.length > 0 && (
          <div>
            <h2 className="text-lg font-semibold mb-3">Saved Reports</h2>
            <div className="space-y-2">
              {pastReports.map(r => (
                <Card key={r.id}>
                  <CardContent className="flex items-center justify-between py-3 px-4">
                    <div className="flex items-center gap-3">
                      <FileText className="h-5 w-5 text-muted-foreground shrink-0" />
                      <div>
                        <p className="font-medium text-sm">{r.title}</p>
                        <p className="text-xs text-muted-foreground">
                          {new Date(r.created_at).toLocaleDateString("en-GB", {
                            day: "numeric", month: "short", year: "numeric",
                          })}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant="secondary" className="text-[10px] capitalize">{r.report_type}</Badge>
                      <Button
                        variant="ghost" size="sm"
                        onClick={() => {
                          fetch(`/api/reports/${r.id}/`, { credentials: "include" })
                            .then(res => res.json())
                            .then(data => {
                              setSelectedType(data.type as ReportType);
                              setReportText(data.content);
                              setStreamError("");
                            });
                        }}
                      >
                        <ChevronRight className="h-4 w-4" />
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          </div>
        )}

        {pastReports.length === 0 && (
          <div className="flex flex-col items-center justify-center text-center py-12 px-4">
            <div className="relative">
              <div className="absolute inset-0 bg-gradient-to-r from-primary/10 to-primary/5 rounded-full blur-3xl animate-pulse" />
              <div className="relative mb-6">
                <div className="w-20 h-20 mx-auto bg-gradient-to-br from-primary/20 to-primary/5 rounded-2xl flex items-center justify-center border border-primary/10 shadow-lg">
                  <FileText className="h-10 w-10 text-primary/60" strokeWidth={1.5} />
                </div>
                <div className="absolute -top-1 -right-1 w-3 h-3 bg-primary/40 rounded-full animate-ping" />
                <div className="absolute -bottom-1 -left-1 w-2 h-2 bg-primary/30 rounded-full" />
              </div>
            </div>

            <h3 className="text-lg font-semibold text-foreground mb-2">
              No Reports Yet
            </h3>
            
            <p className="text-sm text-muted-foreground max-w-md mb-6">
              Generate your first AI-powered real estate report to get started.
              Select a report type above and configure your parameters including the time period.
            </p>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 max-w-2xl mb-8">
              <div className="flex items-center gap-2 text-xs text-muted-foreground bg-muted/30 px-3 py-2 rounded-lg">
                <Sparkles className="h-3.5 w-3.5 text-primary" />
                <span>AI-powered analysis</span>
              </div>
              <div className="flex items-center gap-2 text-xs text-muted-foreground bg-muted/30 px-3 py-2 rounded-lg">
                <BarChart3 className="h-3.5 w-3.5 text-primary" />
                <span>Live market data</span>
              </div>
              <div className="flex items-center gap-2 text-xs text-muted-foreground bg-muted/30 px-3 py-2 rounded-lg">
                <Calendar className="h-3.5 w-3.5 text-primary" />
                <span>Select any month/year</span>
              </div>
            </div>

            <Button 
              variant="outline" 
              size="sm"
              className="gap-2 hover:bg-primary/5"
              onClick={() => {
                document.querySelector('.grid-cols-1.md\\:grid-cols-2')?.scrollIntoView({ behavior: 'smooth' });
              }}
            >
              <Sparkles className="h-3.5 w-3.5" />
              Generate Your First Report
            </Button>
          </div>
        )}
      </div>
    </UserDashboardLayout>
  );
}