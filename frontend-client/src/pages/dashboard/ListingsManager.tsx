import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { ReliabilityBadge } from "@/components/Badges";
import { Download, Search, Loader2, ChevronLeft, ChevronRight } from "lucide-react";
import { toast } from "sonner";
import { useListingsAdmin } from "@/hooks/useAdminData";

function formatPrice(p: number | null) {
  if (!p) return "—";
  return p.toLocaleString("fr-TN") + " TND";
}

function formatDate(s: string | null) {
  if (!s) return "—";
  return new Date(s).toLocaleDateString("fr-TN");
}

export default function ListingsManager() {
  const [search,      setSearch]      = useState("");
  const [source,      setSource]      = useState("");
  const [propType,    setPropType]    = useState("");
  const [transaction, setTransaction] = useState("");
  const [page,        setPage]        = useState(1);
  const [detail,      setDetail]      = useState<any>(null);

  const { data, isLoading } = useListingsAdmin({
    q:           search  || undefined,
    type:        propType || undefined,
    transaction: transaction || undefined,
    page,
    page_size:   50,
  });

  const listings  = data?.results  ?? [];
  const totalPages = data?.pages   ?? 1;
  const total      = data?.count   ?? 0;

  const exportCsv = () => {
    const headers = ["id","source_name","title","city","type","price","rooms","surface","reliability_level"];
    const rows    = listings.map((l: any) => headers.map(h => l[h] ?? ""));
    const csv     = [headers, ...rows].map(r => r.join(",")).join("\n");
    const blob    = new Blob([csv], { type: "text/csv" });
    const a       = document.createElement("a");
    a.href        = URL.createObjectURL(blob);
    a.download    = "listings.csv";
    a.click();
    toast.success("CSV exported");
  };

  return (
    <DashboardLayout>
      <div className="space-y-4">

        {/* Toolbar */}
        <div className="flex flex-wrap gap-3 items-center">
          <div className="relative flex-1 min-w-48">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input className="pl-9" placeholder="Search title or city…"
              value={search}
              onChange={e => { setSearch(e.target.value); setPage(1); }} />
          </div>

          <Select value={propType} onValueChange={v => { setPropType(v === "all" ? "" : v); setPage(1); }}>
            <SelectTrigger className="w-36"><SelectValue placeholder="Type" /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              <SelectItem value="apartment">Apartment</SelectItem>
              <SelectItem value="house">House</SelectItem>
              <SelectItem value="land">Land</SelectItem>
              <SelectItem value="commercial">Commercial</SelectItem>
            </SelectContent>
          </Select>

          <Select value={transaction} onValueChange={v => { setTransaction(v === "all" ? "" : v); setPage(1); }}>
            <SelectTrigger className="w-36"><SelectValue placeholder="Transaction" /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All</SelectItem>
              <SelectItem value="sale">Sale</SelectItem>
              <SelectItem value="rent">Rent</SelectItem>
            </SelectContent>
          </Select>

          <Button variant="outline" onClick={exportCsv} className="gap-2">
            <Download className="h-4 w-4" /> Export CSV
          </Button>
        </div>

        {/* Table */}
        <Card>
          <CardContent className="p-0">
            {isLoading ? (
              <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-left text-muted-foreground">
                      {["Source","Title","City","Type","Price","Rooms","Surface","Reliability","Scraped"].map(h => (
                        <th key={h} className="p-3 whitespace-nowrap">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {listings.map((l: any) => (
                      <tr key={l.id}
                        className="border-b last:border-0 hover:bg-muted/50 cursor-pointer"
                        onClick={() => setDetail(l)}>
                        <td className="p-3">
                          <Badge variant="outline" className="capitalize text-xs">
                            {l.source_name}
                          </Badge>
                        </td>
                        <td className="p-3 max-w-[200px] truncate">{l.title}</td>
                        <td className="p-3">{l.city ?? "—"}</td>
                        <td className="p-3 capitalize">{l.type ?? "—"}</td>
                        <td className="p-3 font-medium">{formatPrice(l.price)}</td>
                        <td className="p-3">{l.rooms ?? "—"}</td>
                        <td className="p-3">{l.surface ? `${l.surface} m²` : "—"}</td>
                        <td className="p-3">
                          <ReliabilityBadge level={l.reliability_level} />
                        </td>
                        <td className="p-3 text-xs whitespace-nowrap">
                          {formatDate(l.scraped_at)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Pagination */}
        <div className="flex items-center justify-between">
          <span className="text-sm text-muted-foreground">{total.toLocaleString()} listings</span>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm"
              disabled={page === 1} onClick={() => setPage(p => p - 1)}>
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <span className="text-sm">Page {page} of {totalPages}</span>
            <Button variant="outline" size="sm"
              disabled={page >= totalPages} onClick={() => setPage(p => p + 1)}>
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      </div>

      {/* Detail sheet */}
      <Sheet open={!!detail} onOpenChange={() => setDetail(null)}>
        <SheetContent className="overflow-y-auto w-full sm:max-w-lg">
          {detail && (
            <>
              <SheetHeader>
                <SheetTitle className="text-base">{detail.title}</SheetTitle>
              </SheetHeader>
              <div className="space-y-4 mt-4">
                {detail.images?.[0]?.url && (
                  <img src={detail.images[0].url} alt=""
                    className="w-full aspect-video object-cover rounded-lg"
                    onError={e => { (e.target as HTMLImageElement).style.display = "none"; }} />
                )}
                <div className="grid grid-cols-2 gap-3 text-sm">
                  {[
                    ["Price",       formatPrice(detail.price)],
                    ["City",        detail.city ?? "—"],
                    ["Type",        detail.type ?? "—"],
                    ["Rooms",       detail.rooms ?? "—"],
                    ["Surface",     detail.surface ? `${detail.surface} m²` : "—"],
                    ["Source",      detail.source_name],
                    ["Transaction", detail.transaction_type ?? "—"],
                    ["Outlier",     detail.is_outlier ? "Yes" : "No"],
                    ["Duplicate",   detail.suspected_duplicate ? "Yes" : "No"],
                    ["Scraped",     formatDate(detail.scraped_at)],
                  ].map(([k, v]) => (
                    <div key={k}>
                      <span className="text-muted-foreground">{k}: </span>
                      <strong className="capitalize">{v}</strong>
                    </div>
                  ))}
                </div>
                <div>
                  <ReliabilityBadge level={detail.reliability_level} />
                  {detail.reliability_score != null && (
                    <span className="ml-2 text-sm text-muted-foreground">
                      {detail.reliability_score?.toFixed(0)}%
                    </span>
                  )}
                </div>
                {detail.description && (
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    {detail.description}
                  </p>
                )}
                {detail.url && (
                  <a href={detail.url} target="_blank" rel="noopener noreferrer"
                    className="text-sm text-primary hover:underline">
                    View original listing →
                  </a>
                )}
              </div>
            </>
          )}
        </SheetContent>
      </Sheet>
    </DashboardLayout>
  );
}