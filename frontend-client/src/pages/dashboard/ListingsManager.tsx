import { useState, useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { mockListings, formatPrice, formatDate, type Listing } from "@/lib/mock-data";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { ReliabilityBadge, FraudScoreBadge } from "@/components/Badges";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Checkbox } from "@/components/ui/checkbox";
import { Download, Search } from "lucide-react";
import { toast } from "sonner";

const PAGE_SIZE = 50;

export default function ListingsManager() {
  const [search, setSearch] = useState("");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [typeFilter, setTypeFilter] = useState("all");
  const [fraudFilter, setFraudFilter] = useState("all");
  const [page, setPage] = useState(1);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [detailListing, setDetailListing] = useState<Listing | null>(null);
  const [sortCol, setSortCol] = useState<string>("scraped_at");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const filtered = useMemo(() => {
    let r = [...mockListings];
    if (search) r = r.filter(l => l.title.toLowerCase().includes(search.toLowerCase()) || l.city.toLowerCase().includes(search.toLowerCase()));
    if (sourceFilter !== "all") r = r.filter(l => l.source_name === sourceFilter);
    if (typeFilter !== "all") r = r.filter(l => l.type === typeFilter);
    if (fraudFilter !== "all") r = r.filter(l => fraudFilter === "flagged" ? l.fraud_flag : !l.fraud_flag);

    r.sort((a, b) => {
      const av = (a as any)[sortCol]; const bv = (b as any)[sortCol];
      const cmp = typeof av === "string" ? av.localeCompare(bv) : av - bv;
      return sortDir === "asc" ? cmp : -cmp;
    });
    return r;
  }, [search, sourceFilter, typeFilter, fraudFilter, sortCol, sortDir]);

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const paginated = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  const toggleSort = (col: string) => {
    if (sortCol === col) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortCol(col); setSortDir("asc"); }
  };

  const toggleSelect = (id: string) => setSelected(prev => {
    const next = new Set(prev);
    next.has(id) ? next.delete(id) : next.add(id);
    return next;
  });

  const exportCsv = () => {
    const headers = ["source", "title", "city", "type", "price", "rooms", "surface", "reliability_level", "fraud_flag"];
    const rows = filtered.map(l => headers.map(h => (l as any)[h]));
    const csv = [headers.join(","), ...rows.map(r => r.join(","))].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = "listings.csv"; a.click();
    toast.success("CSV exported");
  };

  const SortHeader = ({ col, label }: { col: string; label: string }) => (
    <th className="pb-2 pr-4 cursor-pointer select-none whitespace-nowrap" onClick={() => toggleSort(col)}>
      {label} {sortCol === col ? (sortDir === "asc" ? "↑" : "↓") : ""}
    </th>
  );

  return (
    <DashboardLayout>
      <div className="space-y-4">
        {/* Toolbar */}
        <div className="flex flex-wrap gap-3 items-center">
          <div className="relative flex-1 min-w-48">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input className="pl-9" placeholder="Search title or city..." value={search} onChange={e => setSearch(e.target.value)} />
          </div>
          <Select value={sourceFilter} onValueChange={setSourceFilter}>
            <SelectTrigger className="w-36"><SelectValue placeholder="Source" /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Sources</SelectItem>
              <SelectItem value="tayara">Tayara</SelectItem>
              <SelectItem value="mubawab">Mubawab</SelectItem>
              <SelectItem value="affare">Affare</SelectItem>
            </SelectContent>
          </Select>
          <Select value={typeFilter} onValueChange={setTypeFilter}>
            <SelectTrigger className="w-36"><SelectValue placeholder="Type" /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              <SelectItem value="apartment">Apartment</SelectItem>
              <SelectItem value="house">House</SelectItem>
              <SelectItem value="land">Land</SelectItem>
              <SelectItem value="commercial">Commercial</SelectItem>
            </SelectContent>
          </Select>
          <Select value={fraudFilter} onValueChange={setFraudFilter}>
            <SelectTrigger className="w-36"><SelectValue placeholder="Fraud" /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All</SelectItem>
              <SelectItem value="flagged">Flagged</SelectItem>
              <SelectItem value="clean">Clean</SelectItem>
            </SelectContent>
          </Select>
          <Button variant="outline" onClick={exportCsv} className="gap-2"><Download className="h-4 w-4" /> Export CSV</Button>
          {selected.size > 0 && (
            <Button variant="secondary" onClick={() => { setSelected(new Set()); toast.success("Marked as reviewed"); }}>
              Mark {selected.size} as reviewed
            </Button>
          )}
        </div>

        {/* Table */}
        <Card>
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left text-muted-foreground">
                    <th className="p-3 w-8"></th>
                    <SortHeader col="source_name" label="Source" />
                    <SortHeader col="title" label="Title" />
                    <SortHeader col="city" label="City" />
                    <SortHeader col="type" label="Type" />
                    <SortHeader col="price" label="Price" />
                    <SortHeader col="rooms" label="Rooms" />
                    <SortHeader col="surface" label="Surface" />
                    <th className="pb-2 pr-4">Reliability</th>
                    <th className="pb-2 pr-4">Fraud</th>
                    <SortHeader col="scraped_at" label="Scraped" />
                  </tr>
                </thead>
                <tbody>
                  {paginated.map(l => (
                    <tr key={l.id} className="border-b last:border-0 hover:bg-muted/50 cursor-pointer" onClick={() => setDetailListing(l)}>
                      <td className="p-3" onClick={e => e.stopPropagation()}>
                        <Checkbox checked={selected.has(l.id)} onCheckedChange={() => toggleSelect(l.id)} />
                      </td>
                      <td className="py-2 pr-4"><Badge variant="outline" className="capitalize text-xs">{l.source_name}</Badge></td>
                      <td className="py-2 pr-4 max-w-[200px] truncate">{l.title}</td>
                      <td className="py-2 pr-4">{l.city}</td>
                      <td className="py-2 pr-4 capitalize">{l.type}</td>
                      <td className="py-2 pr-4 font-medium">{formatPrice(l.price)}</td>
                      <td className="py-2 pr-4">{l.rooms}</td>
                      <td className="py-2 pr-4">{l.surface}m²</td>
                      <td className="py-2 pr-4"><ReliabilityBadge level={l.reliability_level} /></td>
                      <td className="py-2 pr-4">{l.fraud_flag ? <Badge variant="destructive" className="text-xs">Flagged</Badge> : <Badge variant="outline" className="text-xs">Clean</Badge>}</td>
                      <td className="py-2 pr-4 text-xs whitespace-nowrap">{formatDate(l.scraped_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>

        {/* Pagination */}
        <div className="flex items-center justify-between">
          <span className="text-sm text-muted-foreground">{filtered.length} listings</span>
          <div className="flex gap-2">
            <Button variant="outline" size="sm" disabled={page === 1} onClick={() => setPage(p => p - 1)}>Prev</Button>
            <span className="text-sm flex items-center">Page {page} of {totalPages}</span>
            <Button variant="outline" size="sm" disabled={page === totalPages} onClick={() => setPage(p => p + 1)}>Next</Button>
          </div>
        </div>
      </div>

      {/* Detail Sheet */}
      <Sheet open={!!detailListing} onOpenChange={() => setDetailListing(null)}>
        <SheetContent className="overflow-y-auto w-full sm:max-w-lg">
          {detailListing && (
            <>
              <SheetHeader><SheetTitle className="text-lg">{detailListing.title}</SheetTitle></SheetHeader>
              <div className="space-y-4 mt-4">
                <img src={detailListing.images[0]?.url} alt="" className="w-full aspect-video object-cover rounded-lg" />
                <div className="grid grid-cols-2 gap-3 text-sm">
                  <div><span className="text-muted-foreground">Price:</span> <strong>{formatPrice(detailListing.price)}</strong></div>
                  <div><span className="text-muted-foreground">City:</span> {detailListing.city}</div>
                  <div><span className="text-muted-foreground">Type:</span> <span className="capitalize">{detailListing.type}</span></div>
                  <div><span className="text-muted-foreground">Rooms:</span> {detailListing.rooms}</div>
                  <div><span className="text-muted-foreground">Surface:</span> {detailListing.surface}m²</div>
                  <div><span className="text-muted-foreground">Source:</span> {detailListing.source_name}</div>
                  <div><span className="text-muted-foreground">Reliability:</span> <ReliabilityBadge level={detailListing.reliability_level} /></div>
                  <div><span className="text-muted-foreground">Fraud Score:</span> <FraudScoreBadge score={detailListing.fraud_score} /></div>
                </div>
                <p className="text-sm text-muted-foreground">{detailListing.description}</p>
              </div>
            </>
          )}
        </SheetContent>
      </Sheet>
    </DashboardLayout>
  );
}
