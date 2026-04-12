import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { mockListings, formatPrice, formatPricePerM2 } from "@/lib/mock-data";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { FraudScoreBadge } from "@/components/Badges";
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ZAxis } from "recharts";
import { toast } from "sonner";
import { Link } from "react-router-dom";

export default function FraudCenter() {
  const flagged = mockListings.filter(l => l.fraud_flag);
  const outliers = mockListings.filter(l => l.is_outlier);
  const duplicates = mockListings.filter(l => l.suspected_duplicate);

  const normalData = mockListings.filter(l => !l.is_outlier && l.surface > 0).map(l => ({ surface: l.surface, pricePerM2: l.price_per_m2 }));
  const outlierData = outliers.filter(l => l.surface > 0).map(l => ({ surface: l.surface, pricePerM2: l.price_per_m2 }));

  return (
    <DashboardLayout>
      <Tabs defaultValue="flagged" className="space-y-4">
        <TabsList>
          <TabsTrigger value="flagged">Flagged ({flagged.length})</TabsTrigger>
          <TabsTrigger value="outliers">Outliers ({outliers.length})</TabsTrigger>
          <TabsTrigger value="duplicates">Duplicates ({duplicates.length})</TabsTrigger>
        </TabsList>

        <TabsContent value="flagged">
          <Card>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-left text-muted-foreground">
                      <th className="p-3">Title</th><th className="p-3">City</th><th className="p-3">Score</th>
                      <th className="p-3">Reason</th><th className="p-3">Model</th><th className="p-3">Ratio</th><th className="p-3">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {flagged.map(l => (
                      <tr key={l.id} className="border-b last:border-0">
                        <td className="p-3 max-w-[180px] truncate">{l.title}</td>
                        <td className="p-3">{l.city}</td>
                        <td className="p-3"><FraudScoreBadge score={l.fraud_score} /></td>
                        <td className="p-3 text-xs max-w-[200px] truncate">{l.fraud_reason}</td>
                        <td className="p-3 text-xs">{l.fraud_model_used}</td>
                        <td className="p-3 text-xs">{l.room_image_ratio.toFixed(1)}</td>
                        <td className="p-3 space-x-1">
                          <Button asChild size="sm" variant="outline"><Link to={`/listing/${l.id}`}>View</Link></Button>
                          <Button size="sm" variant="ghost" onClick={() => toast.success("Flag cleared")}>Clear</Button>
                          <Button size="sm" variant="destructive" onClick={() => toast.success("Confirmed fraud")}>Confirm</Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="outliers" className="space-y-4">
          <Card>
            <CardHeader><CardTitle className="text-sm">Price per m² vs Surface</CardTitle></CardHeader>
            <CardContent className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <ScatterChart>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis type="number" dataKey="surface" name="Surface" unit="m²" />
                  <YAxis type="number" dataKey="pricePerM2" name="Price/m²" unit=" TND" />
                  <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                  <Scatter name="Normal" data={normalData} fill="hsl(var(--primary))" opacity={0.4} />
                  <Scatter name="Outlier" data={outlierData} fill="hsl(var(--destructive))" />
                </ScatterChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-left text-muted-foreground">
                      <th className="p-3">Title</th><th className="p-3">City</th><th className="p-3">Price/m²</th>
                      <th className="p-3">Flags</th><th className="p-3">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {outliers.map(l => (
                      <tr key={l.id} className="border-b last:border-0">
                        <td className="p-3 max-w-[180px] truncate">{l.title}</td>
                        <td className="p-3">{l.city}</td>
                        <td className="p-3">{formatPricePerM2(l.price_per_m2)}</td>
                        <td className="p-3">{l.outlier_flags.map(f => <Badge key={f} variant="outline" className="text-xs mr-1 text-warning">{f}</Badge>)}</td>
                        <td className="p-3"><Button asChild size="sm" variant="outline"><Link to={`/listing/${l.id}`}>View</Link></Button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="duplicates">
          <Card>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-left text-muted-foreground">
                      <th className="p-3">Title</th><th className="p-3">City</th><th className="p-3">Price</th>
                      <th className="p-3">Source</th><th className="p-3">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {duplicates.map(l => (
                      <tr key={l.id} className="border-b last:border-0">
                        <td className="p-3 max-w-[180px] truncate">{l.title}</td>
                        <td className="p-3">{l.city}</td>
                        <td className="p-3">{formatPrice(l.price)}</td>
                        <td className="p-3 capitalize">{l.source_name}</td>
                        <td className="p-3 space-x-1">
                          <Button asChild size="sm" variant="outline"><Link to={`/listing/${l.id}`}>View</Link></Button>
                          <Button size="sm" variant="ghost" onClick={() => toast.success("Marked as unique")}>Mark Unique</Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </DashboardLayout>
  );
}
