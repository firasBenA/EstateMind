import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { mockListings, formatDate } from "@/lib/mock-data";
import { CheckCircle, XCircle, Clock, Database, Zap } from "lucide-react";

export default function PipelinePage() {
  const sources = ["tayara", "mubawab", "affare"];

  const sourceStats = sources.map(s => {
    const listings = mockListings.filter(l => l.source_name === s);
    const lastScrape = listings.reduce((max, l) => l.scraped_at > max ? l.scraped_at : max, "");
    const oneDayAgo = new Date(new Date(lastScrape).getTime() - 86400000).toISOString();
    const recentCount = listings.filter(l => l.scraped_at > oneDayAgo).length;
    return { source: s, lastScrape, recentCount, total: listings.length };
  });

  const changeTypes: Record<string, number> = {};
  mockListings.forEach(l => { changeTypes[l.change_type] = (changeTypes[l.change_type] || 0) + 1; });

  const nlpEnriched = mockListings.filter(l => l.nlp_enriched).length;
  const normalized = mockListings.filter(l => l.normalized).length;

  const logEvents = mockListings
    .sort((a, b) => b.scraped_at.localeCompare(a.scraped_at))
    .slice(0, 15)
    .map(l => ({ time: l.scraped_at, source: l.source_name, title: l.title, type: l.change_type }));

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Source Status */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {sourceStats.map(s => (
            <Card key={s.source}>
              <CardContent className="p-5">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="font-semibold capitalize text-lg">{s.source}</h3>
                  <Badge variant="outline" className="bg-success/10 text-success">Active</Badge>
                </div>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between"><span className="text-muted-foreground">Last scrape</span><span>{formatDate(s.lastScrape)}</span></div>
                  <div className="flex justify-between"><span className="text-muted-foreground">Last run additions</span><span className="font-medium">{s.recentCount}</span></div>
                  <div className="flex justify-between"><span className="text-muted-foreground">Total listings</span><span>{s.total}</span></div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>

        {/* Change Detection + Enrichment */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-sm">Change Detection</CardTitle></CardHeader>
            <CardContent>
              <div className="space-y-3">
                {Object.entries(changeTypes).map(([type, count]) => (
                  <div key={type} className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      {type === "new" ? <CheckCircle className="h-4 w-4 text-success" /> : type === "updated" ? <Clock className="h-4 w-4 text-secondary" /> : <XCircle className="h-4 w-4 text-muted-foreground" />}
                      <span className="capitalize text-sm">{type}</span>
                    </div>
                    <span className="font-semibold">{count}</span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-sm">Enrichment Status</CardTitle></CardHeader>
            <CardContent className="space-y-4">
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span>NLP Enriched</span>
                  <span>{nlpEnriched} / {mockListings.length}</span>
                </div>
                <div className="h-2 rounded-full bg-muted overflow-hidden">
                  <div className="h-full rounded-full bg-primary" style={{ width: `${(nlpEnriched / mockListings.length) * 100}%` }} />
                </div>
              </div>
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span>Normalized</span>
                  <span>{normalized} / {mockListings.length}</span>
                </div>
                <div className="h-2 rounded-full bg-muted overflow-hidden">
                  <div className="h-full rounded-full bg-secondary" style={{ width: `${(normalized / mockListings.length) * 100}%` }} />
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Log Feed */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-sm">Recent Scrape Events</CardTitle>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button variant="outline" size="sm" disabled className="gap-2">
                  <Zap className="h-4 w-4" /> Trigger Scrape
                </Button>
              </TooltipTrigger>
              <TooltipContent>Connect to pipeline API</TooltipContent>
            </Tooltip>
          </CardHeader>
          <CardContent>
            <div className="space-y-0">
              {logEvents.map((e, i) => (
                <div key={i} className="flex items-start gap-3 py-3 border-b last:border-0">
                  <div className="mt-1 w-2 h-2 rounded-full bg-primary shrink-0" />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 text-sm">
                      <Badge variant="outline" className="capitalize text-xs">{e.source}</Badge>
                      <Badge variant="outline" className={`text-xs ${e.type === "new" ? "text-success" : e.type === "updated" ? "text-secondary" : "text-muted-foreground"}`}>
                        {e.type}
                      </Badge>
                    </div>
                    <p className="text-sm truncate mt-1">{e.title}</p>
                    <p className="text-xs text-muted-foreground">{formatDate(e.time)}</p>
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
