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
  const priceNum = parseInt(form.price) || 0;

  const priceStatus =
    priceNum === 0
      ? "none"
      : priceNum > suggestedMax * 1.2
        ? "bad"
        : priceNum > suggestedMax
          ? "warning"
          : priceNum >= suggestedMin
            ? "good"
            : "low";

  // --- HANDLERS ---

  const handleImageUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.length) {
      setUploading(true);
      const files = Array.from(e.target.files);
      try {
        const { urls, errors } = await storageApi.uploadImages(files);
        if (urls.length > 0) {
          setForm((prev) => ({ ...prev, images: [...prev.images, ...urls] }));
          toast.success(`${urls.length} image(s) uploaded!`);
        }
        if (errors?.length) {
          errors.forEach((err) => toast.error(`Failed: ${err.filename}`));
        }
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

  const generateDescription = async () => {
    if (!form.city || !form.type) {
      toast.error("Please fill in City and Property Type first");
      return;
    }
    setGenerating(true);
    try {
      const result = await listingsApi.generateDescription({
        metadata: {
          property_type: form.type,
          transaction: form.transaction,
          city: form.city,
          surface_m2: form.surface,
          rooms: form.rooms,
          price: form.price,
        },
      });
      setForm((prev) => ({ ...prev, description: result.description }));
      toast.success("Description generated!");
    } catch {
      toast.error("Failed to generate description");
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
              <CardTitle>Images & Description</CardTitle>
            </CardHeader>
            <CardContent className="space-y-6">
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
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <Label>Description</Label>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={generateDescription}
                    disabled={generating || !form.city}
                  >
                    <Wand2 className="h-4 w-4 mr-2" />
                    {generating ? "Generating..." : "Generate with AI"}
                  </Button>
                </div>
                <Textarea
                  value={form.description}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, description: e.target.value }))
                  }
                  placeholder="Describe your property..."
                  rows={6}
                />
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
                Based on your property details, we suggest a fair market price
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {suggestedMin > 0 && (
                <div className="bg-accent rounded-lg p-4 space-y-2">
                  <p className="text-sm font-medium">Suggested Price Range</p>
                  <p className="text-2xl font-bold text-primary">
                    {suggestedMin.toLocaleString()} -{" "}
                    {suggestedMax.toLocaleString()} TND
                  </p>
                  <p className="text-xs text-muted-foreground">
                    Based on {form.surface} m² {form.type} in{" "}
                    {form.city || "your area"} ({form.transaction})
                  </p>
                </div>
              )}
              <div className="space-y-2">
                <Label>Your Price (TND)</Label>
                <Input
                  type="number"
                  value={form.price}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, price: e.target.value }))
                  }
                  placeholder="Enter your price"
                  className="text-lg"
                />
              </div>
              {/* Price Status Alerts (Same as before) */}
              {priceStatus === "good" && (
                <div className="flex items-start gap-3 p-4 rounded-lg bg-green-50 border border-green-200">
                  <CheckCircle2 className="h-5 w-5 text-green-600 shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-green-800">
                      Great price!
                    </p>
                    <p className="text-xs text-green-700">
                      Your listing is competitively priced.
                    </p>
                  </div>
                </div>
              )}
              {priceStatus === "bad" && (
                <div className="flex items-start gap-3 p-4 rounded-lg bg-red-50 border border-red-200">
                  <AlertTriangle className="h-5 w-5 text-red-600 shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-red-800">
                      Price too high
                    </p>
                    <p className="text-xs text-red-700">
                      Listings priced over 20% above market average receive
                      significantly fewer views.
                    </p>
                  </div>
                </div>
              )}
              {/* Add other status checks here if needed */}
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
            <Button onClick={() => setStep((s) => s + 1)}>
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
