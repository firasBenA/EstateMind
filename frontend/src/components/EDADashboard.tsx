import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, PieChart, Pie, AreaChart, Area, Cell,
} from "recharts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronUp, RefreshCw, ShieldCheck, AlertTriangle } from "lucide-react";

// ── Types ────────────────────────────────────────────────────────────────────

interface RegionStat       { region: string; count: number; }
interface PriceStat        { region: string; min_price: number; max_price: number; avg_price: number; }
interface TransactionStat  { transaction_type: string; count: number; }
interface PropertyTypeStat { type: string; count: number; }
interface TopArea          { city: string; count: number; }
interface TrendStat        { date: string; count: number; }
interface PriceM2Stat      { region: string; avg_m2: number; }
interface FeatureStat      { feature: string; count: number; }

interface EDAData {
  region_stats: RegionStat[];
  price_stats: PriceStat[];
  transaction_stats: TransactionStat[];
  property_type_stats: PropertyTypeStat[];
  top_areas: TopArea[];
  trend_stats: TrendStat[];
  price_m2_stats: PriceM2Stat[];
  top_features: FeatureStat[];
}

interface NullStat {
  field: string;
  null_count: number;
  filled_count: number;
  null_pct: number;
  filled_pct: number;
}

interface ScoreDist {
  level: string;
  count: number;
  pct: number;
}

interface QualityData {
  total: number;
  avg_reliability_score: number;
  score_distribution: ScoreDist[];
  null_field_stats: NullStat[];
  nlp_enriched_count: number;
  nlp_fields_filled: { field: string; count: number }[];
  outlier_count: number;
  outlier_pct: number;
  outlier_flag_breakdown: { flag: string; count: number }[];
  duplicate_count: number;
  duplicate_pct: number;
  source_quality: { source: string; total: number; high: number; good: number; low: number; drop: number }[];
}

// ── Helper Functions ────────────────────────────────────────────────────────────

// Format number with thousands separator and round to nearest hundred
const formatPrice = (value: number): string => {
  if (value >= 1000) {
    // Round to nearest hundred and format with thousands separator
    const rounded = Math.round(value / 100) * 100;
    return rounded.toLocaleString('fr-TN') + ' TND';
  }
  return value.toLocaleString('fr-TN') + ' TND';
};

// Format number for m² price
const formatPriceM2 = (value: number): string => {
  if (value >= 1000) {
    const rounded = Math.round(value / 100) * 100;
    return rounded.toLocaleString('fr-TN') + ' TND/m²';
  }
  return value.toLocaleString('fr-TN') + ' TND/m²';
};

// Calculate percentage for pie chart labels
const calculatePercentage = (value: number, total: number): number => {
  if (total === 0) return 0;
  return Math.round((value / total) * 100);
};

// ── Colors ───────────────────────────────────────────────────────────────────

const PASTEL = [
  '#FFB3BA','#FFDFBA','#FFFFBA','#BAFFC9','#BAE1FF',
  '#E2F0CB','#F1CBFF','#D7FFD9','#FFC4E1','#C9F0FF',
];

const SCORE_COLORS: Record<string, string> = {
  HIGH:    '#4ade80',
  GOOD:    '#60a5fa',
  LOW:     '#fbbf24',
  DROP:    '#f87171',
  UNKNOWN: '#94a3b8',
};

const NULL_COLOR_FN = (pct: number) => {
  if (pct >= 80) return '#f87171';
  if (pct >= 50) return '#fbbf24';
  if (pct >= 20) return '#60a5fa';
  return '#4ade80';
};

// ── Sub-components ────────────────────────────────────────────────────────────

function QualitySection({ data }: { data: QualityData | null }) {
  if (!data) return null;

  const totalBar = data.total || 1;

  return (
    <div className="space-y-4">
      {/* Score distribution + avg score */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {/* Avg score card */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <ShieldCheck className="h-4 w-4 text-primary" />
              Avg Reliability Score
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-4xl font-bold text-foreground">{data.avg_reliability_score}</p>
            <p className="text-xs text-muted-foreground mt-1">out of 100 across {data.total.toLocaleString()} listings</p>
            {/* Inline score bar */}
            <div className="mt-4 space-y-1.5">
              {data.score_distribution.map(s => (
                <div key={s.level} className="flex items-center gap-2 text-xs">
                  <span className="w-14 text-right text-muted-foreground">{s.level}</span>
                  <div className="flex-1 h-2 rounded-full bg-secondary overflow-hidden">
                    <div
                      className="h-full rounded-full transition-all duration-700"
                      style={{ width: `${s.pct}%`, backgroundColor: SCORE_COLORS[s.level] || '#94a3b8' }}
                    />
                  </div>
                  <span className="w-10 text-muted-foreground">{s.count}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Outliers + duplicates */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <AlertTriangle className="h-4 w-4 text-warning" />
              Data Issues
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div>
              <div className="flex justify-between text-sm mb-1">
                <span className="text-muted-foreground">Outliers flagged</span>
                <span className="font-medium text-foreground">{data.outlier_count} ({data.outlier_pct}%)</span>
              </div>
              <div className="h-2 rounded-full bg-secondary overflow-hidden">
                <div className="h-full rounded-full bg-warning/70" style={{ width: `${data.outlier_pct}%` }} />
              </div>
            </div>
            <div>
              <div className="flex justify-between text-sm mb-1">
                <span className="text-muted-foreground">Cross-source duplicates</span>
                <span className="font-medium text-foreground">{data.duplicate_count} ({data.duplicate_pct}%)</span>
              </div>
              <div className="h-2 rounded-full bg-secondary overflow-hidden">
                <div className="h-full rounded-full bg-destructive/60" style={{ width: `${data.duplicate_pct}%` }} />
              </div>
            </div>
            <div>
              <div className="flex justify-between text-sm mb-1">
                <span className="text-muted-foreground">NLP-enriched records</span>
                <span className="font-medium text-foreground">
                  {data.nlp_enriched_count} ({data.total > 0 ? Math.round(data.nlp_enriched_count / data.total * 100) : 0}%)
                </span>
              </div>
              <div className="h-2 rounded-full bg-secondary overflow-hidden">
                <div
                  className="h-full rounded-full bg-primary"
                  style={{ width: `${data.total > 0 ? (data.nlp_enriched_count / data.total) * 100 : 0}%` }}
                />
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Source quality stacked */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Quality by Source</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {data.source_quality.slice(0, 6).map(src => {
                const highPct = src.total > 0 ? (src.high / src.total) * 100 : 0;
                const goodPct = src.total > 0 ? (src.good / src.total) * 100 : 0;
                const lowPct  = src.total > 0 ? (src.low  / src.total) * 100 : 0;
                const dropPct = src.total > 0 ? (src.drop / src.total) * 100 : 0;
                return (
                  <div key={src.source} className="flex items-center gap-2 text-xs">
                    <span className="w-20 truncate text-muted-foreground">{src.source}</span>
                    <div className="flex-1 h-3 rounded-full overflow-hidden flex">
                      <div className="h-full bg-green-400/80"  style={{ width: `${highPct}%` }} />
                      <div className="h-full bg-blue-400/80"   style={{ width: `${goodPct}%` }} />
                      <div className="h-full bg-yellow-400/70" style={{ width: `${lowPct}%`  }} />
                      <div className="h-full bg-red-400/70"    style={{ width: `${dropPct}%` }} />
                    </div>
                    <span className="w-8 text-right text-muted-foreground">{src.total}</span>
                  </div>
                );
              })}
              <div className="flex gap-3 mt-2 text-xs text-muted-foreground">
                {[['HIGH','bg-green-400/80'],['GOOD','bg-blue-400/80'],['LOW','bg-yellow-400/70'],['DROP','bg-red-400/70']].map(([l,c]) => (
                  <span key={l} className="flex items-center gap-1">
                    <span className={`inline-block w-2 h-2 rounded-full ${c}`} />{l}
                  </span>
                ))}
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Null field heatmap */}
      <Card>
        <CardHeader>
          <CardTitle>Null Field Analysis</CardTitle>
          <CardDescription>Percentage of listings missing each field — red = critical gap</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4">
            {data.null_field_stats.map(stat => (
              <div key={stat.field} className="rounded-lg border border-border p-3">
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-xs font-medium text-foreground capitalize">{stat.field}</span>
                  <span
                    className="text-xs font-bold"
                    style={{ color: NULL_COLOR_FN(stat.null_pct) }}
                  >
                    {stat.null_pct}%
                  </span>
                </div>
                <div className="h-1.5 rounded-full bg-secondary overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-700"
                    style={{
                      width: `${stat.null_pct}%`,
                      backgroundColor: NULL_COLOR_FN(stat.null_pct),
                    }}
                  />
                </div>
                <p className="text-xs text-muted-foreground mt-1">
                  {stat.filled_count.toLocaleString()} filled
                </p>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Outlier flag breakdown + NLP fields filled */}
      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Outlier Flag Breakdown</CardTitle>
            <CardDescription>Most common reasons listings are flagged</CardDescription>
          </CardHeader>
          <CardContent>
            {data.outlier_flag_breakdown.length === 0 ? (
              <p className="text-sm text-muted-foreground">No outliers detected yet.</p>
            ) : (
              <ResponsiveContainer width="100%" height={200}>
                <BarChart
                  data={data.outlier_flag_breakdown.slice(0, 8)}
                  layout="vertical"
                  margin={{ left: 10 }}
                >
                  <XAxis type="number" fontSize={11} tickLine={false} axisLine={false} />
                  <YAxis
                    dataKey="flag"
                    type="category"
                    width={140}
                    fontSize={10}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={v => v.replace(/_/g, ' ')}
                  />
                  <Tooltip />
                  <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                    {data.outlier_flag_breakdown.slice(0, 8).map((_, i) => (
                      <Cell key={i} fill={PASTEL[i % PASTEL.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>NLP Fields Filled</CardTitle>
            <CardDescription>Fields recovered from description text</CardDescription>
          </CardHeader>
          <CardContent>
            {data.nlp_fields_filled.length === 0 ? (
              <p className="text-sm text-muted-foreground">No NLP enrichment data yet.</p>
            ) : (
              <div className="space-y-2">
                {data.nlp_fields_filled.slice(0, 8).map((item, i) => (
                  <div key={item.field} className="flex items-center gap-3 text-sm">
                    <span className="w-28 text-muted-foreground capitalize">{item.field}</span>
                    <div className="flex-1 h-2 rounded-full bg-secondary overflow-hidden">
                      <div
                        className="h-full rounded-full bg-primary transition-all duration-700"
                        style={{
                          width: `${data.nlp_enriched_count > 0
                            ? Math.min(100, (item.count / data.nlp_enriched_count) * 100)
                            : 0}%`
                        }}
                      />
                    </div>
                    <span className="w-8 text-right text-muted-foreground">{item.count}</span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

// ── Main EDADashboard ─────────────────────────────────────────────────────────

export function EDADashboard() {
  const [data,        setData]        = useState<EDAData | null>(null);
  const [qualityData, setQualityData] = useState<QualityData | null>(null);
  const [loading,     setLoading]     = useState(true);
  const [isOpen,      setIsOpen]      = useState(true);
  const [activeTab,   setActiveTab]   = useState<"eda" | "quality">("eda");

  const fetchData = async () => {
    setLoading(true);
    try {
      const [edaResp, qualResp] = await Promise.all([
        fetch("/api/eda/",     { credentials: "include" }),
        fetch("/api/quality/", { credentials: "include" }),
      ]);
      if (edaResp.ok)  setData(await edaResp.json());
      if (qualResp.ok) setQualityData(await qualResp.json());
    } catch (error) {
      console.error("Failed to fetch dashboard data", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  if (loading && !data) {
    return (
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Analytics Dashboard</CardTitle>
          <CardDescription>Loading data...</CardDescription>
        </CardHeader>
        <CardContent className="h-[200px] flex items-center justify-center">
          <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
        </CardContent>
      </Card>
    );
  }

  if (!data) {
    return (
      <Card className="w-full border-destructive/50">
        <CardHeader>
          <CardTitle className="text-destructive">Error Loading Analytics</CardTitle>
          <CardDescription>
            Unable to fetch data. Ensure the backend is running.
            <Button variant="outline" size="sm" onClick={fetchData} className="ml-4">Retry</Button>
          </CardDescription>
        </CardHeader>
      </Card>
    );
  }

  // Pre-process
  const topPrices = [...(data.price_stats || [])]
    .sort((a, b) => b.avg_price - a.avg_price)
    .slice(0, 12)
    .map(item => ({ ...item, avg_price: Math.round(item.avg_price ) }));

  const topPricesM2 = (data.price_m2_stats || [])
    .slice(0, 12)
    .map(item => ({ ...item, avg_m2: Math.round(item.avg_m2 ) }));

  const topRegions = [...(data.region_stats || [])].sort((a, b) => b.count - a.count).slice(0, 12);
  
  // Calculate total for transaction percentages
  const transactionTotal = data.transaction_stats.reduce((sum, item) => sum + item.count, 0);
  
  // Calculate total for property type percentages
  const propertyTypeTotal = data.property_type_stats.reduce((sum, item) => sum + item.count, 0);

  return (
    <Collapsible open={isOpen} onOpenChange={setIsOpen} className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold tracking-tight">Analytics</h2>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={fetchData} disabled={loading}>
            <RefreshCw className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
          <CollapsibleTrigger asChild>
            <Button variant="ghost" size="sm">
              {isOpen ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
            </Button>
          </CollapsibleTrigger>
        </div>
      </div>

      <CollapsibleContent className="space-y-4">
        {/* Tab switcher */}
        <div className="flex gap-1 rounded-lg bg-secondary/50 p-1 w-fit">
          {(["eda", "quality"] as const).map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${
                activeTab === tab
                  ? "bg-background text-foreground shadow-sm"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              {tab === "eda" ? "Market Analysis" : "Data Quality"}
            </button>
          ))}
        </div>

        {activeTab === "quality" && <QualitySection data={qualityData} />}

        {activeTab === "eda" && (
          <div className="space-y-4">
            {/* Row 1: Regional + Transaction */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
              <Card className="col-span-4">
                <CardHeader>
                  <CardTitle>Regional Distribution</CardTitle>
                  <CardDescription>Listings by governorate</CardDescription>
                </CardHeader>
                <CardContent className="pl-2">
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={topRegions}>
                      <XAxis dataKey="region" fontSize={11} tickLine={false} axisLine={false} />
                      <YAxis fontSize={11} tickLine={false} axisLine={false} />
                      <Tooltip />
                      <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                        {topRegions.map((_, i) => (
                          <Cell key={i} fill={PASTEL[i % PASTEL.length]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>

              <Card className="col-span-3">
                <CardHeader>
                  <CardTitle>Transaction Types</CardTitle>
                  <CardDescription>Sale vs Rent</CardDescription>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={260}>
                    <PieChart>
                      <Pie
                        data={data.transaction_stats}
                        cx="50%" cy="50%"
                        innerRadius={55} outerRadius={80}
                        paddingAngle={4}
                        dataKey="count"
                        nameKey="transaction_type"
                        label={({ transaction_type, count }) => {
                          const pct = calculatePercentage(count, transactionTotal);
                          return `${transaction_type} ${pct}%`;
                        }}
                      >
                        {data.transaction_stats.map((_, i) => (
                          <Cell key={i} fill={PASTEL[i % PASTEL.length]} />
                        ))}
                      </Pie>
                      <Tooltip />
                    </PieChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            </div>

            {/* Row 2: Property types + Price by region */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
              <Card className="col-span-3">
                <CardHeader>
                  <CardTitle>Property Types</CardTitle>
                  <CardDescription>Distribution by category</CardDescription>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={260}>
                    <PieChart>
                      <Pie
                        data={data.property_type_stats}
                        cx="50%" cy="50%"
                        innerRadius={55} outerRadius={80}
                        paddingAngle={3}
                        dataKey="count"
                        nameKey="type"
                        label={({ type, count }) => {
                          const pct = calculatePercentage(count, propertyTypeTotal);
                          return `${type} ${pct}%`;
                        }}
                      >
                        {data.property_type_stats.map((_, i) => (
                          <Cell key={i} fill={PASTEL[(i + 2) % PASTEL.length]} />
                        ))}
                      </Pie>
                      <Tooltip />
                      <Legend verticalAlign="bottom" height={36} />
                    </PieChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>

              <Card className="col-span-4">
                <CardHeader>
                  <CardTitle>Avg Price by Region</CardTitle>
                  <CardDescription>Top 12 regions (thousands TND)</CardDescription>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={topPrices} layout="vertical" margin={{ left: 10 }}>
                      <XAxis type="number" fontSize={11} tickLine={false} axisLine={false} />
                      <YAxis dataKey="region" type="category" width={100} fontSize={11} tickLine={false} axisLine={false} />
                      <Tooltip formatter={(v: number) => formatPrice(v)} />
                      <Bar dataKey="avg_price" name="Avg Price" radius={[0, 4, 4, 0]}>
                        {topPrices.map((_, i) => (
                          <Cell key={i} fill={PASTEL[(i + 3) % PASTEL.length]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            </div>

            {/* Row 3: Price/m2 */}
            <Card>
              <CardHeader>
                <CardTitle>Avg Price per m²</CardTitle>
                <CardDescription>By region (thousands TND/m²)</CardDescription>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={260}>
                  <BarChart data={topPricesM2} layout="vertical">
                    <XAxis type="number" fontSize={11} tickLine={false} axisLine={false} />
                    <YAxis dataKey="region" type="category" width={100} fontSize={11} tickLine={false} axisLine={false} />
                    <Tooltip formatter={(v: number) => formatPriceM2(v)} />
                    <Bar dataKey="avg_m2" radius={[0, 4, 4, 0]}>
                      {topPricesM2.map((_, i) => (
                        <Cell key={i} fill={PASTEL[(i + 4) % PASTEL.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            {/* Row 4: Trend + Top areas */}
            <div className="grid gap-4 md:grid-cols-2">
              <Card>
                <CardHeader>
                  <CardTitle>Listing Frequency</CardTitle>
                  <CardDescription>New listings added per day</CardDescription>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={260}>
                    <AreaChart data={data.trend_stats}>
                      <defs>
                        <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%"  stopColor="#BAE1FF" stopOpacity={0.8} />
                          <stop offset="95%" stopColor="#BAE1FF" stopOpacity={0}   />
                        </linearGradient>
                      </defs>
                      <XAxis dataKey="date" fontSize={11} tickLine={false} axisLine={false} />
                      <YAxis fontSize={11} tickLine={false} axisLine={false} />
                      <Tooltip />
                      <Area type="monotone" dataKey="count" stroke="#BAE1FF" fill="url(#colorCount)" />
                    </AreaChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Top 10 Cities</CardTitle>
                  <CardDescription>Most active areas</CardDescription>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={data.top_areas} layout="vertical">
                      <XAxis type="number" fontSize={11} tickLine={false} axisLine={false} />
                      <YAxis dataKey="city" type="category" width={120} fontSize={11} tickLine={false} axisLine={false} />
                      <Tooltip />
                      <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                        {data.top_areas.map((_, i) => (
                          <Cell key={i} fill={PASTEL[(i + 5) % PASTEL.length]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            </div>

            {/* Row 5: Top features */}
            {data.top_features && data.top_features.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle>Most Common Features</CardTitle>
                  <CardDescription>Property amenities extracted from listings</CardDescription>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={data.top_features.slice(0, 12)}>
                      <XAxis
                        dataKey="feature"
                        fontSize={11}
                        tickLine={false}
                        axisLine={false}
                        tickFormatter={v => v.charAt(0).toUpperCase() + v.slice(1)}
                      />
                      <YAxis fontSize={11} tickLine={false} axisLine={false} />
                      <Tooltip />
                      <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                        {data.top_features.slice(0, 12).map((_, i) => (
                          <Cell key={i} fill={PASTEL[(i + 6) % PASTEL.length]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}
          </div>
        )}
      </CollapsibleContent>
    </Collapsible>
  );
}
