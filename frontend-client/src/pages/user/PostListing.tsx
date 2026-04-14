import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { CITIES } from "@/lib/mock-data";
import { Wand2, Upload, AlertTriangle, CheckCircle2, ArrowLeft, ArrowRight, ImagePlus } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";

const steps = ["Property Details", "Images & Description", "Smart Pricing", "Review & Publish"];

export default function PostListing() {
  const [step, setStep] = useState(0);
  const [form, setForm] = useState({
    title: "", type: "apartment", transaction: "sale", city: "", rooms: "3", surface: "", description: "",
    price: "", images: [] as string[],
  });
  const [generating, setGenerating] = useState(false);

  const suggestedMin = form.surface ? parseInt(form.surface) * 2200 : 0;
  const suggestedMax = form.surface ? parseInt(form.surface) * 3800 : 0;
  const priceNum = parseInt(form.price) || 0;
  const priceStatus = priceNum === 0 ? "none" : priceNum > suggestedMax * 1.2 ? "bad" : priceNum > suggestedMax ? "warning" : priceNum >= suggestedMin ? "good" : "low";

  const generateDescription = () => {
    setGenerating(true);
    setTimeout(() => {
      setForm(f => ({
        ...f,
        description: `Beautiful ${f.type} located in ${f.city || "a prime location"}. This ${f.surface || "spacious"} m² property features ${f.rooms} rooms with modern finishes and excellent natural lighting. Perfect for ${f.transaction === "rent" ? "tenants seeking comfort" : "families or investors"}. Close to amenities, schools, and public transport. Don't miss this opportunity!`
      }));
      setGenerating(false);
      toast.success("Description generated with AI!");
    }, 1500);
  };

  return (
    <UserDashboardLayout>
      <div className="max-w-3xl mx-auto space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Post a New Listing</h1>
          <p className="text-muted-foreground">Fill in the details to publish your property</p>
        </div>

        {/* Progress */}
        <div className="space-y-2">
          <div className="flex justify-between text-sm">
            {steps.map((s, i) => (
              <span key={s} className={`${i === step ? "text-primary font-semibold" : i < step ? "text-success" : "text-muted-foreground"}`}>
                {i < step ? "✓ " : ""}{s}
              </span>
            ))}
          </div>
          <Progress value={((step + 1) / steps.length) * 100} className="h-2" />
        </div>

        {/* Step 1: Details */}
        {step === 0 && (
          <Card>
            <CardHeader><CardTitle>Property Details</CardTitle></CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label>Title</Label>
                <Input value={form.title} onChange={e => setForm(f => ({ ...f, title: e.target.value }))} placeholder="e.g. Modern Apartment in La Marsa" />
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label>Property Type</Label>
                  <Select value={form.type} onValueChange={v => setForm(f => ({ ...f, type: v }))}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="apartment">Apartment</SelectItem>
                      <SelectItem value="house">House</SelectItem>
                      <SelectItem value="land">Land</SelectItem>
                      <SelectItem value="commercial">Commercial</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>Transaction</Label>
                  <Select value={form.transaction} onValueChange={v => setForm(f => ({ ...f, transaction: v }))}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="sale">Sale</SelectItem>
                      <SelectItem value="rent">Rent</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div className="space-y-2">
                  <Label>City</Label>
                  <Select value={form.city} onValueChange={v => setForm(f => ({ ...f, city: v }))}>
                    <SelectTrigger><SelectValue placeholder="Select" /></SelectTrigger>
                    <SelectContent>{CITIES.map(c => <SelectItem key={c} value={c}>{c}</SelectItem>)}</SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>Rooms</Label>
                  <Select value={form.rooms} onValueChange={v => setForm(f => ({ ...f, rooms: v }))}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {["Studio", "1", "2", "3", "4", "5+"].map(r => <SelectItem key={r} value={r}>{r === "Studio" ? r : `S+${r}`}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>Surface (m²)</Label>
                  <Input type="number" value={form.surface} onChange={e => setForm(f => ({ ...f, surface: e.target.value }))} placeholder="120" />
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Step 2: Images & Description */}
        {step === 1 && (
          <Card>
            <CardHeader><CardTitle>Images & Description</CardTitle></CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label>Property Images</Label>
                <div className="border-2 border-dashed rounded-lg p-8 text-center hover:border-primary/50 transition-colors cursor-pointer">
                  <ImagePlus className="h-10 w-10 mx-auto text-muted-foreground mb-3" />
                  <p className="text-sm text-muted-foreground">Click to upload or drag images here</p>
                  <p className="text-xs text-muted-foreground mt-1">PNG, JPG up to 10MB each</p>
                  <Button variant="outline" size="sm" className="mt-3">
                    <Upload className="h-4 w-4 mr-2" /> Choose Files
                  </Button>
                </div>
              </div>
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <Label>Description</Label>
                  <Button variant="outline" size="sm" onClick={generateDescription} disabled={generating}>
                    <Wand2 className="h-4 w-4 mr-2" />
                    {generating ? "Generating..." : "Generate with AI"}
                  </Button>
                </div>
                <Textarea
                  value={form.description}
                  onChange={e => setForm(f => ({ ...f, description: e.target.value }))}
                  placeholder="Describe your property..."
                  rows={6}
                />
                <p className="text-xs text-muted-foreground">AI will generate a professional description based on your property details</p>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Step 3: Pricing */}
        {step === 2 && (
          <Card>
            <CardHeader>
              <CardTitle>Smart Pricing</CardTitle>
              <CardDescription>Based on your property details, we suggest a fair market price</CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {suggestedMin > 0 && (
                <div className="bg-accent rounded-lg p-4 space-y-2">
                  <p className="text-sm font-medium">Suggested Price Range</p>
                  <p className="text-2xl font-bold text-primary">
                    {suggestedMin.toLocaleString()} - {suggestedMax.toLocaleString()} TND
                  </p>
                  <p className="text-xs text-muted-foreground">
                    Based on {form.surface} m² {form.type} in {form.city || "your area"} ({form.transaction})
                  </p>
                </div>
              )}

              <div className="space-y-2">
                <Label>Your Price (TND)</Label>
                <Input
                  type="number"
                  value={form.price}
                  onChange={e => setForm(f => ({ ...f, price: e.target.value }))}
                  placeholder="Enter your price"
                  className="text-lg"
                />
              </div>

              {priceStatus === "good" && (
                <div className="flex items-start gap-3 p-4 rounded-lg bg-success/10 border border-success/30">
                  <CheckCircle2 className="h-5 w-5 text-success shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-success">Great price!</p>
                    <p className="text-xs text-muted-foreground">Your listing is competitively priced and likely to attract buyers quickly.</p>
                  </div>
                </div>
              )}
              {priceStatus === "low" && (
                <div className="flex items-start gap-3 p-4 rounded-lg bg-warning/10 border border-warning/30">
                  <AlertTriangle className="h-5 w-5 text-warning shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-warning">Below market value</p>
                    <p className="text-xs text-muted-foreground">Your price is below the suggested range. You might be undervaluing your property.</p>
                  </div>
                </div>
              )}
              {priceStatus === "warning" && (
                <div className="flex items-start gap-3 p-4 rounded-lg bg-warning/10 border border-warning/30">
                  <AlertTriangle className="h-5 w-5 text-warning shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-warning">Slightly above market</p>
                    <p className="text-xs text-muted-foreground">Your price is slightly above average. Consider adjusting to attract more interest.</p>
                  </div>
                </div>
              )}
              {priceStatus === "bad" && (
                <div className="flex items-start gap-3 p-4 rounded-lg bg-destructive/10 border border-destructive/30">
                  <AlertTriangle className="h-5 w-5 text-destructive shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-destructive">Price too high — you may lose more than you gain</p>
                    <p className="text-xs text-muted-foreground">
                      Listings priced over 20% above market average receive 73% fewer views and take 4x longer to sell.
                      Your listing will be flagged as a potential outlier, reducing trust with buyers.
                    </p>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* Step 4: Review */}
        {step === 3 && (
          <Card>
            <CardHeader><CardTitle>Review & Publish</CardTitle></CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                {[
                  { label: "Title", value: form.title || "—" },
                  { label: "Type", value: form.type },
                  { label: "Transaction", value: form.transaction },
                  { label: "City", value: form.city || "—" },
                  { label: "Rooms", value: form.rooms },
                  { label: "Surface", value: form.surface ? `${form.surface} m²` : "—" },
                  { label: "Price", value: form.price ? `${parseInt(form.price).toLocaleString()} TND` : "—" },
                ].map(item => (
                  <div key={item.label}>
                    <p className="text-xs text-muted-foreground">{item.label}</p>
                    <p className="font-medium text-sm">{item.value}</p>
                  </div>
                ))}
              </div>
              {form.description && (
                <div>
                  <p className="text-xs text-muted-foreground mb-1">Description</p>
                  <p className="text-sm">{form.description}</p>
                </div>
              )}
              {priceStatus === "bad" && (
                <Badge variant="destructive">⚠ Price flagged as above market</Badge>
              )}
            </CardContent>
          </Card>
        )}

        {/* Navigation */}
        <div className="flex justify-between">
          <Button variant="outline" onClick={() => setStep(s => s - 1)} disabled={step === 0}>
            <ArrowLeft className="h-4 w-4 mr-2" /> Back
          </Button>
          {step < steps.length - 1 ? (
            <Button onClick={() => setStep(s => s + 1)}>
              Next <ArrowRight className="h-4 w-4 ml-2" />
            </Button>
          ) : (
            <Button onClick={() => toast.success("Listing published successfully!")}>
              Publish Listing
            </Button>
          )}
        </div>
      </div>
    </UserDashboardLayout>
  );
}
