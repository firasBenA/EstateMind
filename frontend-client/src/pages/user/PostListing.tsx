import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { supabase } from "@/lib/supabase";
import { Textarea } from "@/components/ui/textarea";
import {
  listingsApi,
  storageApi,
  ApiError,
  type CreateListingPayload,
} from "@/lib/api";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { CITIES } from "@/lib/mock-data";
import {
  Wand2,
  Upload,
  AlertTriangle,
  CheckCircle2,
  ArrowLeft,
  ArrowRight,
  ImagePlus,
  X,
  Loader2,
  MapPin,
} from "lucide-react";
import { useState, useRef, useEffect } from "react";
import { toast } from "sonner";

// ️ Leaflet Imports
import { MapContainer, TileLayer, Marker, useMapEvents } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import L from "leaflet";

// Fix for default marker icon in React/Webpack
delete (L.Icon.Default.prototype as any)._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl:
    "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png",
  iconUrl:
    "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png",
  shadowUrl:
    "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png",
});

const steps = [
  "Property Details & Location", // Updated step name
  "Images & Description",
  "Smart Pricing",
  "Review & Publish",
];

// 📍 Helper Component to Handle Map Clicks
function LocationMarker({
  setCoords,
}: {
  setCoords: (lat: number, lng: number) => void;
}) {
  const [position, setPosition] = useState<L.LatLng | null>(null);

  const map = useMapEvents({
    click(e) {
      setPosition(e.latlng);
      setCoords(e.latlng.lat, e.latlng.lng);
      map.flyTo(e.latlng, map.getZoom());
    },
  });

  return position === null ? null : <Marker position={position}></Marker>;
}

export default function PostListing() {
  const [step, setStep] = useState(0);
  const [form, setForm] = useState({
    title: "",
    type: "apartment",
    transaction: "sale",
    city: "",
    rooms: "3",
    surface: "",
    description: "",
    price: "",
    images: [] as string[],
    latitude: null as number | null, // ✅ Added
    longitude: null as number | null, // ✅ Added
  });

  const [generating, setGenerating] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [publishing, setPublishing] = useState(false);

  const fileInputRef = useRef<HTMLInputElement>(null);

  // --- PRICING LOGIC ---
  const suggestedMin = form.surface ? parseInt(form.surface) * 2200 : 0;
  const suggestedMax = form.surface ? parseInt(form.surface) * 3800 : 0;

  // --- HANDLERS ---
  const [tempFiles, setTempFiles] = useState<File[]>([]);

  const handleImageUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.length) {
      setUploading(true);
      const files = Array.from(e.target.files);

      // Keep reference for AI generation
      setTempFiles((prev) => [...prev, ...files]);

      try {
        const { urls, errors } = await storageApi.uploadImages(files);
        if (urls.length > 0) {
          setForm((prev) => ({ ...prev, images: [...prev.images, ...urls] }));
          toast.success(`${urls.length} image(s) uploaded!`);
        }
        // ... error handling ...
      } catch {
        toast.error("Error uploading images");
      } finally {
        setUploading(false);
        if (fileInputRef.current) fileInputRef.current.value = "";
      }
    }
  };

  const removeImage = async (index: number) => {
    const urlToRemove = form.images[index];
    await storageApi.removeImage(urlToRemove);
    setForm((prev) => ({
      ...prev,
      images: prev.images.filter((_, i) => i !== index),
    }));
  };

  // Add this inside the PostListing component, before the return statement
  const generateAIDescription = async () => {
    if (tempFiles.length === 0) {
      toast.error(
        "Please upload at least one image to generate a description.",
      );
      return;
    }
    if (!form.city || !form.type) {
      toast.error("Please fill in City and Property Type first.");
      return;
    }

    setGenerating(true);
    try {
      // Call the REAL API with files
      const result = await listingsApi.generateDescription({
        metadata: {
          property_type: form.type,
          transaction: form.transaction,
          city: form.city,
          surface_m2: form.surface,
          rooms: form.rooms,
          price: form.price,
          // tone: "professional",
        },
        files: tempFiles, // Pass the raw File objects here
      });

      // The API returns the description directly or with specific properties.
      // Use the description property if available, otherwise format highlights.
      const fullDesc = result.description || result.highlights.join(". ");

      setForm((prev) => ({ ...prev, description: fullDesc }));
      toast.success("AI Description generated!");
    } catch (error: any) {
      console.error(error);
      toast.error(error.message || "Failed to generate description.");
    } finally {
      setGenerating(false);
    }
  };

  // frontend-client/src/pages/PostListing.tsx

  const publishListing = async () => {
    setPublishing(true);
    try {
      const payload: CreateListingPayload = {
        title: form.title,
        type: form.type as "apartment" | "house" | "land" | "commercial",
        transaction: form.transaction as "sale" | "rent",
        city: form.city,
        rooms: form.rooms === "Studio" ? 1 : parseInt(form.rooms),
        surface: parseFloat(form.surface) || 0,
        price: parseFloat(form.price) || 0,
        description: form.description,
        images: form.images,
        features: [],
        poi: [],
        latitude: form.latitude,
        longitude: form.longitude,
      };

      // 📝 LOG 1: Check what React is sending
      console.log("🚀 Sending Payload to Django:", payload);
      console.log(
        `📍 Coordinates: Lat=${payload.latitude}, Lng=${payload.longitude}`,
      );

      const result = await listingsApi.create(payload);

      toast.success("✅ Listing published successfully!");
    } catch (error) {
      console.error("Publish error:", error);
      if (error instanceof ApiError) {
        toast.error(error.message);
      } else {
        toast.error("Failed to publish listing. Please try again.");
      }
    } finally {
      setPublishing(false);
    }
  };

  const [prediction, setPrediction] = useState<{
    predicted_price: number;
    price_low: number;
    price_high: number;
    price_per_m2: number;
    margin_pct: number;
    model_used: string;
  } | null>(null);
  const [loadingPrediction, setLoadingPrediction] = useState(false);

  // ── Add this function ────────────────────────────────────────────────────────

  const fetchPricePrediction = async () => {
    if (!form.city || !form.type || !form.surface || !form.transaction) return;

    setLoadingPrediction(true);
    setPrediction(null);
    try {
      // ✅ USE THE API CLIENT INSTEAD OF RAW FETCH
      const data = await listingsApi.predictPrice({
        transaction: form.transaction,
        type: form.type,
        city: form.city,
        surface: parseFloat(form.surface), // Ensure it's a number
        rooms: form.rooms === "Studio" ? 0 : parseInt(form.rooms) || 0,
        images_count: form.images.length,
        has_description: form.description ? 1 : 0,
        desc_length: form.description?.length ?? 0,
        has_coords: form.latitude ? 1 : 0,
      });

      setPrediction(data);
    } catch (error) {
      console.error(error);
      toast.error("Could not fetch price prediction");
    } finally {
      setLoadingPrediction(false);
    }
  };

  // ── Trigger prediction when user reaches step 2 (pricing) ───────────────────
  useEffect(() => {
    if (step === 2) fetchPricePrediction();
  }, [step]);

  // ── Update price validation to use model ceiling ─────────────────────────────
  const priceNum = parseInt(form.price) || 0;
  const hardCeiling = prediction?.price_high ?? null;
  const isBeyondCeiling = hardCeiling !== null && priceNum > hardCeiling;

  const priceStatus =
    priceNum === 0
      ? "none"
      : isBeyondCeiling
        ? "blocked"
        : prediction && priceNum > prediction.predicted_price * 1.1
          ? "warning"
          : prediction && priceNum >= prediction.price_low
            ? "good"
            : priceNum > 0
              ? "low"
              : "none";

  return (
    <UserDashboardLayout>
      <div className="max-w-3xl mx-auto space-y-6 pb-20">
        <div>
          <h1 className="text-2xl font-bold">Post a New Listing</h1>
          <p className="text-muted-foreground">
            Fill in the details to publish your property
          </p>
        </div>

        {/* Progress Bar */}
        <div className="space-y-2">
          <div className="flex justify-between text-sm">
            {steps.map((s, i) => (
              <span
                key={s}
                className={`${
                  i === step
                    ? "text-primary font-semibold"
                    : i < step
                      ? "text-green-600"
                      : "text-muted-foreground"
                }`}
              >
                {i < step ? "✓ " : ""}
                {s}
              </span>
            ))}
          </div>
          <Progress value={((step + 1) / steps.length) * 100} className="h-2" />
        </div>

        {/* STEP 1: DETAILS & LOCATION */}
        {step === 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Property Details & Location</CardTitle>
              <CardDescription>
                Click on the map to set the exact location
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Basic Details */}
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label>Title</Label>
                  <Input
                    value={form.title}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, title: e.target.value }))
                    }
                    placeholder="e.g. Modern Apartment in La Marsa"
                  />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>Property Type</Label>
                    <Select
                      value={form.type}
                      onValueChange={(v) => setForm((f) => ({ ...f, type: v }))}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
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
                    <Select
                      value={form.transaction}
                      onValueChange={(v) =>
                        setForm((f) => ({ ...f, transaction: v }))
                      }
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
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
                    <Select
                      value={form.city}
                      onValueChange={(v) => setForm((f) => ({ ...f, city: v }))}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select" />
                      </SelectTrigger>
                      <SelectContent>
                        {CITIES.map((c) => (
                          <SelectItem key={c} value={c}>
                            {c}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label>Rooms</Label>
                    <Select
                      value={form.rooms}
                      onValueChange={(v) =>
                        setForm((f) => ({ ...f, rooms: v }))
                      }
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {["Studio", "1", "2", "3", "4", "5+"].map((r) => (
                          <SelectItem key={r} value={r}>
                            {r === "Studio" ? r : `S+${r}`}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label>Surface (m²)</Label>
                    <Input
                      type="number"
                      value={form.surface}
                      onChange={(e) =>
                        setForm((f) => ({ ...f, surface: e.target.value }))
                      }
                      placeholder="120"
                    />
                  </div>
                </div>
              </div>

              {/* 🗺️ MAP SECTION */}
              <div className="space-y-2 pt-4 border-t">
                <Label className="flex items-center gap-2">
                  <MapPin className="h-4 w-4" /> Location (Click on map to pin)
                </Label>

                <div className="h-[300px] w-full rounded-md overflow-hidden border z-0 relative">
                  {/* Default center: Tunis, Tunisia */}
                  <MapContainer
                    center={[36.8065, 10.1815]}
                    zoom={12}
                    scrollWheelZoom={false}
                    className="h-full w-full"
                  >
                    <TileLayer
                      attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                      url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    />
                    <LocationMarker
                      setCoords={(lat, lng) =>
                        setForm((f) => ({
                          ...f,
                          latitude: lat,
                          longitude: lng,
                        }))
                      }
                    />
                  </MapContainer>
                </div>

                {/* Display Coordinates */}
                {form.latitude && form.longitude && (
                  <div className="flex gap-4 text-xs text-muted-foreground bg-accent p-2 rounded">
                    <span>Lat: {form.latitude.toFixed(4)}</span>
                    <span>Lng: {form.longitude.toFixed(4)}</span>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        )}
        {/* STEP 2: IMAGES & DESCRIPTION */}
        {step === 1 && (
          <Card>
            <CardHeader>
              <CardTitle>Images & AI Description</CardTitle>
              <CardDescription>
                Upload images and let AI write the perfect description
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Image Upload Section (Existing) */}
              <div className="space-y-2">
                <Label>Property Images</Label>
                <input
                  type="file"
                  multiple
                  accept="image/*"
                  ref={fileInputRef}
                  onChange={handleImageUpload}
                  className="hidden"
                />
                <div
                  className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors cursor-pointer hover:border-primary/50 hover:bg-accent/50 ${uploading ? "opacity-50 pointer-events-none" : ""}`}
                  onClick={() => fileInputRef.current?.click()}
                >
                  {uploading ? (
                    <Loader2 className="h-10 w-10 mx-auto text-primary mb-3 animate-spin" />
                  ) : (
                    <ImagePlus className="h-10 w-10 mx-auto text-muted-foreground mb-3" />
                  )}
                  <p className="text-sm font-medium">
                    {uploading ? "Uploading..." : "Click to upload images"}
                  </p>
                  <p className="text-xs text-muted-foreground mt-1">
                    PNG, JPG up to 10MB each
                  </p>
                </div>

                {/* Image Previews */}
                {form.images.length > 0 && (
                  <div className="grid grid-cols-4 gap-4 mt-4">
                    {form.images.map((url, idx) => (
                      <div key={idx} className="relative aspect-square group">
                        <img
                          src={url}
                          alt="Preview"
                          className="w-full h-full object-cover rounded-md border"
                        />
                        <button
                          onClick={() => removeImage(idx)}
                          className="absolute top-1 right-1 bg-red-500 text-white p-1 rounded-full opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          <X className="h-3 w-3" />
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* AI Description Generator Section */}
              <div className="space-y-2 pt-4 border-t">
                <div className="flex items-center justify-between">
                  <Label>AI-Generated Description</Label>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={generateAIDescription}
                    disabled={
                      generating || form.images.length === 0 || !form.city
                    }
                  >
                    <Wand2 className="h-4 w-4 mr-2" />
                    {generating ? "Writing..." : "Generate with AI"}
                  </Button>
                </div>

                <Textarea
                  value={form.description}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, description: e.target.value }))
                  }
                  placeholder="AI will generate a professional description based on your images and details..."
                  rows={8}
                  className="bg-slate-50 dark:bg-slate-900"
                />

                {form.description && (
                  <p className="text-xs text-green-600 flex items-center gap-1">
                    <CheckCircle2 className="h-3 w-3" /> Description ready for
                    review
                  </p>
                )}
              </div>
            </CardContent>
          </Card>
        )}

        {/* STEP 3: PRICING */}
        {step === 2 && (
          <Card>
            <CardHeader>
              <CardTitle>Smart Pricing</CardTitle>
              <CardDescription>
                Our AI model estimates a fair market price based on your
                property
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* AI Prediction Panel */}
              {loadingPrediction ? (
                <div className="flex items-center gap-3 p-4 rounded-lg bg-accent">
                  <Loader2 className="h-5 w-5 animate-spin text-primary" />
                  <p className="text-sm text-muted-foreground">
                    Running price model...
                  </p>
                </div>
              ) : prediction ? (
                <div className="rounded-lg border bg-accent/60 p-4 space-y-3">
                  <div className="flex items-center justify-between">
                    <p className="text-sm font-semibold">AI Price Estimate</p>
                    <Badge variant="outline" className="text-xs">
                      Model: {prediction.model_used} · ±{prediction.margin_pct}%
                    </Badge>
                  </div>

                  {/* Price band visual */}
                  <div className="flex items-end gap-3">
                    <div className="text-center">
                      <p className="text-xs text-muted-foreground mb-1">Min</p>
                      <p className="text-sm font-medium text-green-700">
                        {prediction.price_low.toLocaleString()}
                      </p>
                    </div>
                    <div className="flex-1 text-center">
                      <p className="text-xs text-muted-foreground mb-1">
                        Predicted
                      </p>
                      <p className="text-2xl font-bold text-primary">
                        {prediction.predicted_price.toLocaleString()} TND
                      </p>
                    </div>
                    <div className="text-center">
                      <p className="text-xs text-muted-foreground mb-1">
                        Max{" "}
                        <span className="text-red-500 font-bold">
                          (ceiling)
                        </span>
                      </p>
                      <p className="text-sm font-medium text-red-600">
                        {prediction.price_high.toLocaleString()}
                      </p>
                    </div>
                  </div>

                  {/* Visual bar */}
                  <div className="relative h-2 rounded-full bg-gradient-to-r from-green-300 via-blue-400 to-red-400">
                    {priceNum > 0 && hardCeiling && (
                      <div
                        className={`absolute top-1/2 -translate-y-1/2 h-4 w-4 rounded-full border-2 border-white shadow transition-all ${
                          isBeyondCeiling ? "bg-red-600" : "bg-primary"
                        }`}
                        style={{
                          left: `${Math.min(
                            ((priceNum - prediction.price_low) /
                              (hardCeiling - prediction.price_low)) *
                              100,
                            100,
                          )}%`,
                        }}
                      />
                    )}
                  </div>

                  <p className="text-xs text-muted-foreground">
                    {prediction.price_per_m2?.toLocaleString()} TND/m² ·{" "}
                    {form.surface} m² {form.type} in {form.city}
                  </p>
                </div>
              ) : null}

              {/* Price Input */}
              <div className="space-y-2">
                <Label>Your Price (TND)</Label>
                <Input
                  type="number"
                  value={form.price}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, price: e.target.value }))
                  }
                  placeholder="Enter your price"
                  className={`text-lg ${isBeyondCeiling ? "border-red-500 focus-visible:ring-red-500" : ""}`}
                />
                {hardCeiling && (
                  <p className="text-xs text-muted-foreground">
                    Maximum allowed:{" "}
                    <span className="font-semibold text-red-600">
                      {hardCeiling.toLocaleString()} TND
                    </span>
                  </p>
                )}
              </div>

              {/* Status alerts */}
              {priceStatus === "blocked" && (
                <div className="flex items-start gap-3 p-4 rounded-lg bg-red-50 border border-red-300">
                  <AlertTriangle className="h-5 w-5 text-red-600 shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-semibold text-red-800">
                      Price exceeds market ceiling
                    </p>
                    <p className="text-xs text-red-700">
                      Our model caps this listing at{" "}
                      <strong>{hardCeiling?.toLocaleString()} TND</strong>.
                      Please lower your price to proceed.
                    </p>
                  </div>
                </div>
              )}
              {priceStatus === "good" && (
                <div className="flex items-start gap-3 p-4 rounded-lg bg-green-50 border border-green-200">
                  <CheckCircle2 className="h-5 w-5 text-green-600 shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-green-800">
                      Great price!
                    </p>
                    <p className="text-xs text-green-700">
                      Competitively priced within the market range.
                    </p>
                  </div>
                </div>
              )}
              {priceStatus === "warning" && (
                <div className="flex items-start gap-3 p-4 rounded-lg bg-yellow-50 border border-yellow-200">
                  <AlertTriangle className="h-5 w-5 text-yellow-600 shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-yellow-800">
                      Slightly above estimate
                    </p>
                    <p className="text-xs text-yellow-700">
                      Your price is above the predicted value. This may reduce
                      visibility.
                    </p>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* STEP 4: REVIEW */}
        {step === 3 && (
          <Card>
            <CardHeader>
              <CardTitle>Review & Publish</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                {[
                  { label: "Title", value: form.title || "—" },
                  { label: "Type", value: form.type },
                  { label: "Transaction", value: form.transaction },
                  { label: "City", value: form.city || "—" },
                  { label: "Rooms", value: form.rooms },
                  {
                    label: "Surface",
                    value: form.surface ? `${form.surface} m²` : "—",
                  },
                  {
                    label: "Price",
                    value: form.price
                      ? `${parseInt(form.price).toLocaleString()} TND`
                      : "—",
                  },
                  {
                    label: "Location",
                    value:
                      form.latitude && form.longitude
                        ? `${form.latitude.toFixed(2)}, ${form.longitude.toFixed(2)}`
                        : "Not set",
                  },
                ].map((item) => (
                  <div key={item.label}>
                    <p className="text-xs text-muted-foreground">
                      {item.label}
                    </p>
                    <p className="font-medium text-sm">{item.value}</p>
                  </div>
                ))}
              </div>
              {form.images.length > 0 && (
                <div>
                  <p className="text-xs text-muted-foreground mb-1">
                    Images ({form.images.length})
                  </p>
                  <div className="flex gap-2 overflow-x-auto pb-2">
                    {form.images.map((img, i) => (
                      <img
                        key={i}
                        src={img}
                        className="h-16 w-16 object-cover rounded border"
                      />
                    ))}
                  </div>
                </div>
              )}
              {form.description && (
                <div>
                  <p className="text-xs text-muted-foreground mb-1">
                    Description
                  </p>
                  <p className="text-sm italic text-gray-600">
                    "{form.description}"
                  </p>
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* NAVIGATION BUTTONS */}
        <div className="flex justify-between pt-4 border-t">
          <Button
            variant="outline"
            onClick={() => setStep((s) => s - 1)}
            disabled={step === 0 || publishing}
          >
            <ArrowLeft className="h-4 w-4 mr-2" /> Back
          </Button>
          {step < steps.length - 1 ? (
            <Button
              onClick={() => setStep((s) => s + 1)}
              disabled={step === 2 && (isBeyondCeiling || !form.price)}
            >
              Next <ArrowRight className="h-4 w-4 ml-2" />
            </Button>
          ) : (
            <Button
              onClick={publishListing}
              disabled={
                publishing || !form.title || !form.city || !form.surface
              }
            >
              {publishing ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />{" "}
                  Publishing...
                </>
              ) : (
                "Publish Listing"
              )}
            </Button>
          )}
        </div>
      </div>
    </UserDashboardLayout>
  );
}
