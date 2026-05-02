// frontend-client/src/pages/LandingPage.tsx

import { useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { PublicLayout }   from "@/components/PublicLayout";
import { HeroScene }      from "@/components/HeroScene";
import { ListingCard }    from "@/components/ListingCard";
import { Button }         from "@/components/ui/button";
import { Skeleton }       from "@/components/ui/skeleton";
import Recommendations from "@/components/recommendation";
import { useAuth } from "@/lib/auth-context";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { useListings, useListingsMeta } from "@/hooks/useListings";
import {
  Search, Building2, MapPin, TrendingUp, CalendarDays,
  Shield, BarChart3, FileText, ArrowRight,
  Users, Zap, Sparkles, Target, Clock, Flame,
} from "lucide-react";

export default function LandingPage() {
  const navigate = useNavigate();
  const { isAuthenticated } = useAuth();
  
  // ── Search bar state ──────────────────────────────────────────────────────
  const [city,      setCity]      = useState("");
  const [transType, setTransType] = useState("");
  const [propType,  setPropType]  = useState("");
  
  // ── Toggle state for featured/recommended ─────────────────────────────────
  const [activeSection, setActiveSection] = useState<"featured" | "recommended">("featured");

  const handleSearch = () => {
    const params = new URLSearchParams();
    if (city)      params.set("city",        city);
    if (transType) params.set("transaction", transType);
    if (propType)  params.set("type",        propType);
    navigate(`/search?${params.toString()}`);
  };

  // ── Data from API ─────────────────────────────────────────────────────────
  const { meta, loading: metaLoading } = useListingsMeta();

  const { listings: featuredListings, loading: featuredLoading } = useListings({
    page:      1,
    page_size: 6,
    sort:      "recent",
    fraud:     false,
  });

  const CITIES = meta?.cities ?? [];

  const stats = [
    {
      icon: Building2, label: "Active Listings",
      value: metaLoading ? null : (meta?.total_listings ?? 0).toLocaleString("en-US"),
      color: "bg-primary/10 text-primary",
    },
    {
      icon: MapPin, label: "Cities Covered",
      value: metaLoading ? null : String(meta?.cities_covered ?? 0),
      color: "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400",
    },
    {
      icon: TrendingUp, label: "Avg. Price/m²",
      value: metaLoading ? null : `${(meta?.avg_price_per_m2 ?? 0).toLocaleString("en-US")} TND`,
      color: "bg-amber-500/10 text-amber-600 dark:text-amber-400",
    },
    {
      icon: CalendarDays, label: "Added This Week",
      value: metaLoading ? null : String(meta?.listings_this_week ?? 0),
      color: "bg-violet-500/10 text-violet-600 dark:text-violet-400",
    },
  ];

  return (
    <PublicLayout>
      {/* ── Hero ──────────────────────────────────────────────────────────── */}
      <section className="relative overflow-hidden bg-gradient-to-b from-primary/5 via-background to-background">
        <div className="container mx-auto px-4 pt-8 md:pt-12">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 items-center">
            {/* Left: headline */}
            <div className="space-y-6 text-center lg:text-left relative z-10">
              <div className="inline-flex items-center gap-2 rounded-full border bg-card px-4 py-1.5 text-sm text-muted-foreground">
                <Zap className="h-3.5 w-3.5 text-primary" />
                Real-time data from 3+ sources
              </div>
              <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold tracking-tight leading-tight">
                Find your property in{" "}
                <span className="text-gradient-brand">Tunisia</span>
              </h1>
              <p className="text-lg text-muted-foreground max-w-lg">
                Search, analyze and invest intelligently with AI — aggregated data from Tayara, Mubawab, Affare and more.
              </p>
              <div className="flex flex-wrap gap-3 justify-center lg:justify-start">
                <Button size="lg" onClick={() => navigate("/search")} className="gap-2">
                  <Search className="h-4 w-4" /> Explore listings
                </Button>
                <Button size="lg" variant="outline" asChild>
                  <Link to="/register">Get started <ArrowRight className="h-4 w-4 ml-1" /></Link>
                </Button>
              </div>
            </div>

            {/* Right: hero scene */}
            <div className="relative">
              <HeroScene />
            </div>
          </div>
        </div>
      </section>

      {/* ── Stats ─────────────────────────────────────────────────────────── */}
      <section className="pt-16 pb-10">
        <div className="container mx-auto px-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {metaLoading
              ? Array.from({ length: 4 }).map((_, i) => <div key={i}><Skeleton className="h-24 w-full" /></div>)
              : stats.map(s => (
                  <div key={s.label}
                    className="flex items-center gap-4 rounded-2xl border bg-card p-4 shadow-sm hover:shadow-md transition-shadow">
                    <div className={`h-12 w-12 rounded-xl ${s.color} flex items-center justify-center shrink-0`}>
                      <s.icon className="h-5 w-5" />
                    </div>
                    <div>
                      <div className="text-xl font-bold">{s.value}</div>
                      <div className="text-xs text-muted-foreground">{s.label}</div>
                    </div>
                  </div>
                ))
            }
          </div>
        </div>
      </section>

      {/* ── How it works ──────────────────────────────────────────────────── */}
      <section className="container mx-auto px-4 py-16">
        <div className="text-center mb-14">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-primary mb-2">Simple Process</span>
          <h2 className="text-3xl md:text-4xl font-bold">How It Works</h2>
          <p className="text-muted-foreground max-w-xl mx-auto mt-3">
            Whether you're buying, renting or investing — three steps to your next property.
          </p>
        </div>
        <div className="relative max-w-4xl mx-auto">
          <div className="hidden md:block absolute top-12 left-[16%] right-[16%] h-0.5 bg-border" />
          <div className="grid grid-cols-1 md:grid-cols-3 gap-10">
            {[
              { icon: Search,   num: "01", title: "Search & Discover",    desc: "Browse thousands of listings aggregated from all major Tunisian platforms with smart filters." },
              { icon: BarChart3,num: "02", title: "Analyze & Compare",     desc: "Get AI-powered price insights, fraud detection, and profitability analysis." },
              { icon: FileText, num: "03", title: "Act & Close",           desc: "Post listings, generate contracts, and negotiate — all from one unified platform." },
            ].map((item, i) => (
              <div key={i} className="relative text-center group">
                <div className="relative z-10 h-16 w-16 rounded-full bg-primary text-primary-foreground flex items-center justify-center mx-auto text-lg font-bold shadow-lg shadow-primary/25 group-hover:scale-110 transition-transform">
                  {item.num}
                </div>
                <div className="mt-5 space-y-2">
                  <div className="flex items-center justify-center gap-2">
                    <item.icon className="h-4 w-4 text-primary" />
                    <h3 className="text-lg font-semibold">{item.title}</h3>
                  </div>
                  <p className="text-sm text-muted-foreground leading-relaxed">{item.desc}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── TOGGLE SECTION: Featured / AI Recommendations ─────────────────── */}
      <section className="container mx-auto px-4 py-10">
        {/* Toggle Buttons */}
        <div className="flex justify-center mb-10">
          <div className="inline-flex rounded-lg border bg-card p-1 shadow-sm">
            <button
              onClick={() => setActiveSection("featured")}
              className={`flex items-center gap-2 px-6 py-2.5 rounded-md text-sm font-medium transition-all ${
                activeSection === "featured"
                  ? "bg-primary text-primary-foreground shadow-sm"
                  : "text-muted-foreground hover:bg-muted"
              }`}
            >
              <Flame className="h-4 w-4" />
              Featured Listings
            </button>
            <button
              onClick={() => setActiveSection("recommended")}
              className={`flex items-center gap-2 px-6 py-2.5 rounded-md text-sm font-medium transition-all ${
                activeSection === "recommended"
                  ? "bg-primary text-primary-foreground shadow-sm"
                  : "text-muted-foreground hover:bg-muted"
              }`}
            >
              <Sparkles className="h-4 w-4" />
              Recommendations
            </button>
          </div>
        </div>

        {/* Featured Listings Section */}
        {activeSection === "featured" && (
          <div>
            <div className="flex items-center justify-between mb-6">
              <div>
                <h2 className="text-2xl font-bold">Featured Listings</h2>
                <p className="text-muted-foreground text-sm mt-1">
                  Latest listings published on EstateMind
                </p>
              </div>
              <Button variant="ghost" onClick={() => navigate("/search")} className="gap-1">
                View all <ArrowRight className="h-4 w-4" />
              </Button>
            </div>

            {featuredLoading ? (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {Array.from({ length: 6 }).map((_, i) => (
                  <div key={i} className="rounded-xl border bg-card overflow-hidden">
                    <Skeleton className="aspect-[4/3] w-full" />
                    <div className="p-4 space-y-2">
                      <Skeleton className="h-4 w-3/4" />
                      <Skeleton className="h-3 w-1/2" />
                      <Skeleton className="h-5 w-1/3" />
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {featuredListings.map(l => <ListingCard key={l.id} listing={l} />)}
              </div>
            )}
          </div>
        )}

        {/* AI Recommendations Section (only for logged-in users) */}
        {activeSection === "recommended" && (
          <div>
            {isAuthenticated ? (
              <Recommendations />
            ) : (
              <div className="text-center py-16 bg-muted/30 rounded-2xl">
                <Sparkles className="h-12 w-12 mx-auto text-primary/50 mb-4" />
                <h3 className="text-xl font-semibold mb-2">Sign in to see your recommendations</h3>
                <p className="text-muted-foreground mb-6">
                  Create an account or sign in to get personalized recommendations based on your searches.
                </p>
                <div className="flex gap-3 justify-center">
                  <Button asChild variant="outline">
                    <Link to="/login">Sign in</Link>
                  </Button>
                  <Button asChild>
                    <Link to="/register">Create account</Link>
                  </Button>
                </div>
              </div>
            )}
          </div>
        )}
      </section>

      {/* ── Why EstateMind ────────────────────────────────────────────────── */}
      <section className="container mx-auto px-4 py-20">
        <div className="text-center mb-12">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-primary mb-2">Our Advantage</span>
          <h2 className="text-3xl md:text-4xl font-bold">Why Choose EstateMind?</h2>
          <p className="text-muted-foreground max-w-xl mx-auto mt-3">
            AI-powered analysis combined with multi-source data for the most transparent view of Tunisia's real estate market.
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 max-w-5xl mx-auto">
          {[
            { icon: Shield,   title: "Fraud Detection",     desc: "We automatically flag suspicious listings.", span: "md:col-span-2" },
            { icon: TrendingUp,title: "Price Intelligence",  desc: "Know if a listing is fairly priced.", span: "" },
            { icon: Target,   title: "AI Matching",         desc: "Personalized recommendations based on your criteria.", span: "" },
            { icon: Sparkles, title: "AI Descriptions",      desc: "Generate listings and contracts in one click.", span: "" },
            { icon: Users,    title: "For Everyone",        desc: "Individuals, agencies and investors — one platform.", span: "" },
            { icon: Clock,    title: "Real-time Updates",   desc: "New listings appear within minutes.", span: "md:col-span-2" },
          ].map((item, i) => (
            <div key={i} className={`group rounded-2xl border bg-card p-6 hover:shadow-lg hover:border-primary/20 transition-all ${item.span}`}>
              <div className="h-10 w-10 rounded-xl bg-primary/10 flex items-center justify-center mb-4 group-hover:bg-primary/20 transition-colors">
                <item.icon className="h-5 w-5 text-primary" />
              </div>
              <h3 className="font-semibold mb-1">{item.title}</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">{item.desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ── CTA ───────────────────────────────────────────────────────────── */}
      <section className="container mx-auto px-4 pb-20">
        <div className="relative overflow-hidden rounded-3xl p-10 md:p-16 text-white"
          style={{ background: "linear-gradient(135deg, hsl(231,72%,52%) 0%, hsl(220,80%,42%) 40%, hsl(260,70%,48%) 100%)" }}>
          <div className="absolute inset-0 pointer-events-none">
            <div className="absolute -top-14 -right-14 w-80 h-80 rounded-full bg-white/[0.18] blur-[60px]" />
            <div className="absolute -bottom-12 -left-10 w-64 h-64 rounded-full blur-[50px]"
              style={{ background: "rgba(180,160,255,0.22)" }} />
            <div className="absolute top-[40%] left-[40%] w-44 h-44 rounded-full blur-[40px]"
              style={{ background: "rgba(100,200,255,0.12)" }} />
          </div>
          <div className="relative z-10 max-w-2xl mx-auto text-center space-y-6">
            <h2 className="text-3xl md:text-4xl font-bold text-white">Ready to list your property?</h2>
            <p className="text-lg text-white/90">
              Post your listing in minutes with AI descriptions, smart pricing, and instant visibility in Tunisia.
            </p>
            <div className="flex flex-wrap gap-3 justify-center">
              <Button size="lg" asChild
                className="bg-white/95 text-primary hover:bg-white shadow-lg shadow-black/20">
                <Link to="/user/post-listing">Post a listing</Link>
              </Button>
              <Button size="lg" asChild
                className="bg-white/10 text-white border border-white/40 hover:bg-white/20 backdrop-blur-sm">
                <Link to="/search">Browse listings</Link>
              </Button>
            </div>
          </div>
        </div>
      </section>

    </PublicLayout>
  );
}