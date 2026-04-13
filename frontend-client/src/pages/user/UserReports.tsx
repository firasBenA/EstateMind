/**
 * frontend-client/src/pages/user/UserReports.tsx
 *
 * Report generation page.
 * - Param forms per report type
 * - SSE streaming from /api/reports/generate/
 * - Token-by-token display as LLM writes
 * - Save report to backend
 * - Copy to clipboard / basic PDF export
 */
import { useState, useRef, useEffect, useCallback } from "react";
import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button }   from "@/components/ui/button";
import { Input }    from "@/components/ui/input";
import { Label }    from "@/components/ui/label";
import { Badge }    from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { toast }    from "sonner";
import { useAuth }  from "@/lib/auth-context";
import { useListingsMeta } from "@/hooks/useListings";
import {
  FileText, Download, Loader2, TrendingUp, MapPin,
  BarChart3, Copy, Save, ChevronRight, Sparkles, AlertCircle,
} from "lucide-react";

// ── Types ─────────────────────────────────────────────────────────────────────

type ReportType = "market" | "investment";

interface MarketParams {
  city:             string;
  transaction_type: string;
}

interface InvestmentParams {
  city:             string;
  property_type:    string;
  transaction_type: string;
  budget_min:       string;
  budget_max:       string;
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
  {
    id:    "portfolio" as ReportType,
    title: "Portfolio Performance",
    desc:  "Track your saved listings: estimated value, gains, diversity breakdown.",
    icon:  BarChart3,
    color: "text-emerald-600",
    badge: "Coming soon",
    disabled: true,
  },
] as const;

const PROP_TYPES = ["apartment", "house", "land", "commercial"];
const PROP_LABELS: Record<string, string> = {
  apartment: "Apartment", house: "Villa / House",
  land: "Land", commercial: "Commercial",
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function buildTitle(type: ReportType, marketParams: MarketParams, investmentParams:InvestmentParams): string {
  const date = new Date().toLocaleDateString("en-GB", { month: "short", year: "numeric" });
  if (type === "market") {
    const city = marketParams.city ? ` — ${marketParams.city}` : "";
    return `Market Overview${city} · ${date}`;
  }
  const city = marketParams.city ? ` — ${marketParams.city}` : "";
  const pt   = investmentParams.property_type ? ` · ${PROP_LABELS[investmentParams.property_type] ?? investmentParams.property_type}` : "";
  return `Investment Analysis${city}${pt} · ${date}`;
}

// Simple markdown → readable text renderer (no extra deps)
function MarkdownBlock({ text }: { text: string }) {
  // Convert **bold**, ## headings, bullet points to HTML-ish spans
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
        if (line.startsWith("**") && line.endsWith("**")) {
          return <p key={i} className="font-semibold text-foreground">{line.slice(2, -2)}</p>;
        }
        if (line.trim() === "") return <div key={i} className="h-2" />;
        // Inline bold
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
  const { user }                  = useAuth();
  const { meta }                  = useListingsMeta();
  const CITIES                    = meta?.cities ?? [];

  // Report selection
  const [selectedType, setSelectedType] = useState<ReportType | null>(null);

  // Params
  const [marketParams, setMarketParams]         = useState<MarketParams>({ city: "", transaction_type: "" });
  const [investmentParams, setInvestmentParams] = useState<InvestmentParams>({
    city: "", property_type: "", transaction_type: "sale", budget_min: "", budget_max: "",
  });

  // Generation state
  const [generating, setGenerating] = useState(false);
  const [reportText, setReportText] = useState("");
  const [streamError, setStreamError] = useState("");
  const [saving, setSaving]         = useState(false);
  const [savedId, setSavedId]       = useState<number | null>(null);

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

  // ── Generate ───────────────────────────────────────────────────────────────
  const handleGenerate = useCallback(async () => {
    if (!selectedType) return;

    setGenerating(true);
    setReportText("");
    setStreamError("");
    setSavedId(null);

    const params =
      selectedType === "market"
        ? { ...marketParams }
        : { ...investmentParams, budget_min: Number(investmentParams.budget_min) || 0, budget_max: Number(investmentParams.budget_max) || 5_000_000 };

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
    // Use raw string params for title, keep numeric conversion only for the API body
   
    const title = buildTitle(selectedType, marketParams,investmentParams);
    const params = selectedType === "market"
      ? marketParams
      : { ...investmentParams, budget_min: Number(investmentParams.budget_min) || 0, budget_max: Number(investmentParams.budget_max) || 5_000_000 };
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

  const handleExportTxt = () => {
    const blob = new Blob([reportText], { type: "text/plain" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href     = url;
    a.download = `${buildTitle(selectedType, marketParams,investmentParams)}.txt`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <UserDashboardLayout>
      <div className="space-y-8 max-w-8xl">

        {/* Header */}
        <div>
          <h1 className="text-2xl font-bold">AI Reports</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Generate data-grounded real estate reports using live market data and a local AI model.
          </p>
        </div>

        {/* Step 1 — Pick type */}
        <div>
          <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground mb-3">
            Step 1 — Choose report type
          </p>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {REPORT_TYPES.map(t => (
              <button
                key={t.id}
                disabled={"disabled" in t && t.disabled}
                onClick={() => { setSelectedType(t.id); setReportText(""); setStreamError(""); }}
                className={`text-left rounded-xl border p-4 transition-all
                  ${"disabled" in t && t.disabled
                    ? "opacity-40 cursor-not-allowed bg-muted/30"
                    : selectedType === t.id
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
        {selectedType && (selectedType === "market" || selectedType === "investment") && (
          <div>
            <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground mb-3">
              Step 2 — Configure parameters
            </p>
            <Card>
              <CardContent className="pt-5 space-y-4">
                {selectedType === "market" && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="space-y-1.5">
                      <Label>City <span className="text-muted-foreground text-xs">(optional)</span></Label>
                      <Select value={marketParams.city || "all"} onValueChange={v => setMarketParams(p => ({ ...p, city: v === "all" ? "" : v }))}>
                        <SelectTrigger><SelectValue placeholder="All Tunisia" /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="all">All Tunisia</SelectItem>
                          {CITIES.map(c => <SelectItem key={c} value={c}>{c}</SelectItem>)}
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
                )}

                {selectedType === "investment" && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="space-y-1.5">
                      <Label>City</Label>
                      <Select value={investmentParams.city || "all"} onValueChange={v => setInvestmentParams(p => ({ ...p, city: v === "all" ? "" : v }))}>
                        <SelectTrigger><SelectValue placeholder="All Tunisia" /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="all">All Tunisia</SelectItem>
                          {CITIES.map(c => <SelectItem key={c} value={c}>{c}</SelectItem>)}
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
                    The local AI is writing your report — this takes 30–90 seconds with gemma3:4b.
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
                  <Button variant="outline" size="sm" onClick={handleCopy} className="gap-1.5">
                    <Copy className="h-3.5 w-3.5" /> Copy
                  </Button>
                  <Button variant="outline" size="sm" onClick={handleExportTxt} className="gap-1.5">
                    <Download className="h-3.5 w-3.5" /> Export
                  </Button>
                  <Button
                    size="sm" onClick={handleSave}
                    disabled={saving || !!savedId}
                    className="gap-1.5"
                  >
                    <Save className="h-3.5 w-3.5" />
                    {savedId ? "Saved ✓" : saving ? "Saving…" : "Save"}
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
                        <MarkdownBlock text={reportText} />
                        {generating && (
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
                {/* Animated background circle */}
                <div className="absolute inset-0 bg-gradient-to-r from-primary/10 to-primary/5 rounded-full blur-3xl animate-pulse" />
                
                {/* Icon container */}
                <div className="relative mb-6">
                  <div className="w-20 h-20 mx-auto bg-gradient-to-br from-primary/20 to-primary/5 rounded-2xl flex items-center justify-center border border-primary/10 shadow-lg">
                    <FileText className="h-10 w-10 text-primary/60" strokeWidth={1.5} />
                  </div>
                  {/* Decorative dots */}
                  <div className="absolute -top-1 -right-1 w-3 h-3 bg-primary/40 rounded-full animate-ping" />
                  <div className="absolute -bottom-1 -left-1 w-2 h-2 bg-primary/30 rounded-full" />
                </div>
              </div>

              {/* Main message */}
              <h3 className="text-lg font-semibold text-foreground mb-2">
                No Reports Yet
              </h3>
              
              <p className="text-sm text-muted-foreground max-w-md mb-6">
                Generate your first AI-powered real estate report to get started. Choose a report type above and configure your parameters.
              </p>

              {/* Feature highlights */}
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
                  <TrendingUp className="h-3.5 w-3.5 text-primary" />
                  <span>Investment insights</span>
                </div>
              </div>

              {/* CTA button (optional) */}
              <Button 
                variant="outline" 
                size="sm"
                className="gap-2 hover:bg-primary/5"
                onClick={() => {
                  // Scroll to report type selection
                  document.querySelector('.grid-cols-1.md\\:grid-cols-3')?.scrollIntoView({ 
                    behavior: 'smooth' 
                  });
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