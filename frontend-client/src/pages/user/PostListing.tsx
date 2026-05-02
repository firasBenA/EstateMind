// frontend-client/src/pages/user/PostListing.tsx

import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  listingsApi,
  locationsApi,
  storageApi,
  ApiError,
  type CreateListingPayload,
  type Governorate,
  type Delegation,
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
import {
  Wand2,
  AlertTriangle,
  CheckCircle2,
  ArrowLeft,
  ArrowRight,
  ImagePlus,
  X,
  Loader2,
  MapPin,
  Info,
} from "lucide-react";
import { useState, useRef, useEffect } from "react";
import { toast } from "sonner";

// Pure-Leaflet map component
import { ClientMap, ClientMapRef } from "@/components/ClientMap";

const steps = [
  "Property Details & Location",
  "Images & Description",
  "Smart Pricing",
  "Review & Publish",
];

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
    latitude: null as number | null,
    longitude: null as number | null,
    delegation_id: null as number | null,
    custom_delegation: "",
    region: "",
    municipality: "",
    zone: "",
  });

  const [generating, setGenerating] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [publishing, setPublishing] = useState(false);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const mapRef = useRef<ClientMapRef>(null);
  const [tempFiles, setTempFiles] = useState<File[]>([]);

  // Location state
  const [governorates, setGovernorates] = useState<Governorate[]>([]);
  const [delegations, setDelegations] = useState<Delegation[]>([]);
  const [selectedGovernorateId, setSelectedGovernorateId] = useState<number | null>(null);
  const [selectedGovernorateName, setSelectedGovernorateName] = useState("");
  const [selectedDelegationId, setSelectedDelegationId] = useState<number | null>(null);
  const [showCustomDelegation, setShowCustomDelegation] = useState(false);
  const [customDelegation, setCustomDelegation] = useState("");
  const [autoCorrectInfo, setAutoCorrectInfo] = useState<any>(null);

  // Validation state
  const [titleValid, setTitleValid] = useState<boolean | null>(null);
  const [titleMessage, setTitleMessage] = useState("");
  const [titleValidating, setTitleValidating] = useState(false);
  const [step1Valid, setStep1Valid] = useState(false);

  // Load governorates on mount
  useEffect(() => {
  const loadGovernorates = async () => {
    try {
      const data = await locationsApi.getGovernorates();
      // The backend now returns latitude/longitude
      setGovernorates(data);
    } catch (error) {
      console.error("Failed to load governorates:", error);
      toast.error("Could not load governorates list");
    }
  };
  loadGovernorates();
}, []);

  // Load delegations when governorate changes
  useEffect(() => {
  if (selectedGovernorateId) {
    const loadDelegations = async () => {
      try {
        console.log(`🔍 Loading delegations for governorate ID: ${selectedGovernorateId}`);
        const data = await locationsApi.getDelegations(selectedGovernorateId);
        console.log(`✅ Received ${data.length} delegations:`, data);
        
        // Important: Set the delegations state
        setDelegations(data);
        
        // Also check what's in the state after setting
        console.log(`📊 Delegations state updated with ${data.length} items`);
        
        if (data.length === 0) {
          console.warn(`⚠️ No delegations found for governorate ${selectedGovernorateId}`);
          setShowCustomDelegation(true);
        } else {
          // Log the first delegation to see structure
          console.log("First delegation sample:", data[0]);
        }
      } catch (error) {
        console.error("Failed to load delegations:", error);
        toast.error("Could not load delegations for this governorate");
      }
    };
    loadDelegations();
  } else {
    setDelegations([]);
  }
}, [selectedGovernorateId]);

  // Validate title on change (debounced)
  useEffect(() => {
    const timer = setTimeout(async () => {
      if (form.title && form.title.length >= 5) {
        setTitleValidating(true);
        try {
          const result = await listingsApi.validateTitle(form.title);
          setTitleValid(result.valid);
          setTitleMessage(result.message);
        } catch (error) {
          console.error("Title validation error:", error);
        } finally {
          setTitleValidating(false);
        }
      } else {
        setTitleValid(null);
        setTitleMessage("");
      }
    }, 500);
    return () => clearTimeout(timer);
  }, [form.title]);

  // Validate Step 1 before allowing next
  useEffect(() => {
    let valid = true;

    // Title validation
    if (!form.title || form.title.length < 5 || titleValid === false) {
      valid = false;
    }

    // Property type
    if (!form.type) valid = false;

    // Transaction type
    if (!form.transaction) valid = false;

    // Governorate
    if (!selectedGovernorateId) valid = false;

    // Delegation (either selected from dropdown OR custom entered)
    const hasDelegation = selectedDelegationId || (showCustomDelegation && customDelegation.trim().length > 0);
    if (!hasDelegation) valid = false;

    // Surface
    if (!form.surface || parseFloat(form.surface) <= 0) valid = false;

    // Coordinates
    if (!form.latitude || !form.longitude) valid = false;

    setStep1Valid(valid);
  }, [
    form.title,
    titleValid,
    form.type,
    form.transaction,
    selectedGovernorateId,
    selectedDelegationId,
    showCustomDelegation,
    customDelegation,
    form.surface,
    form.latitude,
    form.longitude,
  ]);

  // Auto-correct delegation when user types in custom input
  const handleCustomDelegationChange = async (value: string) => {
    setCustomDelegation(value);
    setForm((f) => ({ ...f, custom_delegation: value, city: value }));

    if (value.length >= 3 && selectedGovernorateId) {
      try {
        const result = await locationsApi.autoCorrectDelegation(value, selectedGovernorateId);
        setAutoCorrectInfo(result);

        if (result.matched && result.corrected) {
          // Auto-fill coordinates from matched delegation
          if (result.latitude && result.longitude) {
            setForm((f) => ({
              ...f,
              latitude: result.latitude,
              longitude: result.longitude,
            }));
            if (mapRef.current) {
              mapRef.current.flyTo(result.latitude, result.longitude, 14);
            }
          }
          toast.info(`Did you mean "${result.corrected}"?`, { duration: 3000 });
        }
      } catch (error) {
        console.error("Auto-correct error:", error);
      }
    } else {
      setAutoCorrectInfo(null);
    }
  };

  // Handle governorate selection
  const handleGovernorateChange = (value: string) => {
  const govId = parseInt(value);
  const gov = governorates.find((g) => g.id === govId);
  
  setSelectedGovernorateId(govId);
  setSelectedGovernorateName(gov?.name || "");
  setForm((f) => ({ ...f, region: gov?.name || "" }));
  setSelectedDelegationId(null);
  setShowCustomDelegation(false);
  setCustomDelegation("");
  setAutoCorrectInfo(null);
  
  // 🗺️ Fly map to governorate coordinates if available
  if (gov?.latitude && gov?.longitude && mapRef.current) {
    mapRef.current.flyTo(gov.latitude, gov.longitude, 11);
    // Also set the location pin at governorate center
    setForm((f) => ({ 
      ...f, 
      latitude: gov.latitude, 
      longitude: gov.longitude 
    }));
  } else if (delegations.length > 0 && delegations[0]?.latitude && delegations[0]?.longitude && mapRef.current) {
    // Fallback: use first delegation's coordinates
    const firstDel = delegations[0];
    mapRef.current.flyTo(firstDel.latitude, firstDel.longitude, 12);
    setForm((f) => ({ 
      ...f, 
      latitude: firstDel.latitude, 
      longitude: firstDel.longitude 
    }));
  }
};

  // Handle delegation selection from dropdown
  const handleDelegationChange = (value: string) => {
    if (value === "other") {
      setShowCustomDelegation(true);
      setSelectedDelegationId(null);
      setForm((f) => ({ ...f, delegation_id: null, custom_delegation: "" }));
    } else {
      const delId = parseInt(value);
      const delegation = delegations.find((d) => d.id === delId);
      if (delegation) {
        setSelectedDelegationId(delId);
        setShowCustomDelegation(false);
        setCustomDelegation("");
        setForm((f) => ({
          ...f,
          delegation_id: delId,
          custom_delegation: "",
          city: delegation.name.split("(")[0].trim(),
          municipality: delegation.name,
          latitude: delegation.latitude || f.latitude,
          longitude: delegation.longitude || f.longitude,
        }));
        setAutoCorrectInfo(null);

        // Fly map to delegation coordinates
        if (mapRef.current && delegation.latitude && delegation.longitude) {
          mapRef.current.flyTo(delegation.latitude, delegation.longitude, 14);
        }
      }
    }
  };

  const handleImageUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.length) {
      setUploading(true);
      const files = Array.from(e.target.files);
      setTempFiles((prev) => [...prev, ...files]);

      try {
        const { urls, errors } = await storageApi.uploadImages(files);
        if (urls.length > 0) {
          setForm((prev) => ({ ...prev, images: [...prev.images, ...urls] }));
          toast.success(`${urls.length} image(s) uploaded!`);
        }
        if (errors?.length) {
          toast.error(`${errors.length} image(s) failed to upload`);
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

  const generateAIDescription = async () => {
    if (tempFiles.length === 0) {
      toast.error("Please upload at least one image to generate a description.");
      return;
    }
    if (!form.city || !form.type) {
      toast.error("Please fill in City and Property Type first.");
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
        files: tempFiles,
      });
      const fullDesc = result.description || result.highlights?.join(". ") || "";
      setForm((prev) => ({ ...prev, description: fullDesc }));
      toast.success("AI Description generated!");
    } catch (error: any) {
      console.error(error);
      toast.error(error.message || "Failed to generate description.");
    } finally {
      setGenerating(false);
    }
  };

const publishListing = async () => {
  setPublishing(true);
  try {
    const payload: CreateListingPayload = {
      title: form.title,
      type: form.type as "apartment" | "house" | "land" | "commercial",
      transaction: form.transaction as "sale" | "rent",
      city: showCustomDelegation ? customDelegation : (delegations.find(d => d.id === selectedDelegationId)?.name || form.city),
      rooms: form.rooms === "Studio" ? 1 : parseInt(form.rooms),
      surface: parseFloat(form.surface) || 0,
      price: parseFloat(form.price) || 0,
      description: form.description,
      images: form.images,
      features: [],
      poi: [],
      latitude: form.latitude,
      longitude: form.longitude,
      governorate: selectedGovernorateName,
      region: selectedGovernorateName,  // You can map this based on governorate
      zone: "",  // You can derive this (North/South/Center) based on governorate
      municipality: selectedGovernorateName,
    };

    const result = await listingsApi.create(payload);
    
    if (result.auto_corrected && result.auto_correct_info?.message) {
      toast.info(result.auto_correct_info.message);
    }
    
    toast.success("✅ Listing published successfully!");
    // Optionally redirect to the listing page
    // navigate(`/listings/${result.listing_id}`);
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

  const fetchPricePrediction = async () => {
    if (!form.city || !form.type || !form.surface || !form.transaction) return;

    setLoadingPrediction(true);
    setPrediction(null);
    try {
      const data = await listingsApi.predictPrice({
        transaction: form.transaction,
        type: form.type,
        city: form.city,
        surface: parseFloat(form.surface),
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

  useEffect(() => {
    if (step === 2) fetchPricePrediction();
  }, [step]);

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

  const handleLocationSelect = (lat: number, lng: number) => {
    setForm((f) => ({ ...f, latitude: lat, longitude: lng }));
  };

  return (
    <UserDashboardLayout>
      <div className="max-w-3xl mx-auto space-y-6 pb-20">
        <div>
          <h1 className="text-2xl font-bold">Post a New Listing</h1>
          <p className="text-muted-foreground">
            Fill in the details to publish your property. Title is validated by AI.
          </p>
        </div>

        {/* Progress Bar */}
        <div className="space-y-2">
          <div className="flex justify-between text-sm">
            {steps.map((s, i) => (
              <span
                key={s}
                className={
                  i === step
                    ? "text-primary font-semibold"
                    : i < step
                    ? "text-green-600"
                    : "text-muted-foreground"
                }
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
                Fill all fields below. Title will be validated by AI. Click on map to set exact location.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-4">
                {/* TITLE FIELD */}
                <div className="space-y-2">
                  <Label htmlFor="title">Title *</Label>
                  <Input
                    id="title"
                    value={form.title}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, title: e.target.value }))
                    }
                    placeholder="e.g. Modern apartment in La Marsa with sea view"
                    className={
                      titleValid === false
                        ? "border-red-500"
                        : titleValid === true
                        ? "border-green-500"
                        : ""
                    }
                  />
                  {titleValidating && (
                    <p className="text-xs text-blue-500 flex items-center gap-1">
                      <Loader2 className="h-3 w-3 animate-spin" /> Validating
                      title with AI...
                    </p>
                  )}
                  {titleValid === false && (
                    <p className="text-xs text-red-500">{titleMessage}</p>
                  )}
                  {titleValid === true && (
                    <p className="text-xs text-green-500 flex items-center gap-1">
                      <CheckCircle2 className="h-3 w-3" />{" "}
                      {titleMessage || "Title looks good"}
                    </p>
                  )}
                </div>

                {/* PROPERTY TYPE & TRANSACTION TYPE */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>Property Type *</Label>
                    <Select
                      value={form.type}
                      onValueChange={(v) =>
                        setForm((f) => ({ ...f, type: v }))
                      }
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select type" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="apartment">
                          Apartment / Appartement
                        </SelectItem>
                        <SelectItem value="house">House / Villa / Maison</SelectItem>
                        <SelectItem value="land">Land / Terrain</SelectItem>
                        <SelectItem value="commercial">
                          Commercial / Bureau
                        </SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label>Transaction Type *</Label>
                    <Select
                      value={form.transaction}
                      onValueChange={(v) =>
                        setForm((f) => ({ ...f, transaction: v }))
                      }
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select transaction" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="sale">Sale / Vente</SelectItem>
                        <SelectItem value="rent">Rent / Location</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {/* GOVERNORATE & DELEGATION */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>Governorate / Region *</Label>
                    <Select
                      value={selectedGovernorateId?.toString() || ""}
                      onValueChange={handleGovernorateChange}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select governorate" />
                      </SelectTrigger>
                      <SelectContent className="max-h-60">
                        {governorates.map((gov) => (
                          <SelectItem key={gov.id} value={gov.id.toString()}>
                            {gov.name} ({gov.name_ar})
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="space-y-2">
                    <Label>Delegation / Area *</Label>
                    {!showCustomDelegation ? (
                      <Select
                        value={selectedDelegationId?.toString() || ""}
                        onValueChange={handleDelegationChange}
                        disabled={!selectedGovernorateId}
                      >
                        <SelectTrigger>
                          <SelectValue
                            placeholder={
                              selectedGovernorateId
                                ? "Select delegation"
                                : "Select governorate first"
                            }
                          />
                        </SelectTrigger>
                        <SelectContent className="max-h-60">
                          {delegations.map((del) => (
                            <SelectItem key={del.id} value={del.id.toString()}>
                              {del.name}
                            </SelectItem>
                          ))}
                          <SelectItem value="other">
                            ➕ Other (not in list)
                          </SelectItem>
                        </SelectContent>
                      </Select>
                    ) : (
                      <div className="space-y-2">
                        <Input
                          value={customDelegation}
                          onChange={(e) =>
                            handleCustomDelegationChange(e.target.value)
                          }
                          placeholder="Enter delegation name"
                          className={
                            autoCorrectInfo?.matched ? "border-green-500" : ""
                          }
                        />
                        {autoCorrectInfo?.matched && autoCorrectInfo.corrected && (
                          <div className="p-2 bg-green-50 border border-green-200 rounded-lg">
                            <p className="text-xs text-green-700 flex items-center gap-1">
                              <CheckCircle2 className="h-3 w-3" />
                              Auto-corrected to: {autoCorrectInfo.corrected} (
                              {Math.round(autoCorrectInfo.confidence * 100)}%
                              match)
                            </p>
                          </div>
                        )}
                        {autoCorrectInfo && !autoCorrectInfo.matched && (
                          <p className="text-xs text-yellow-600">
                            ⚠️ {autoCorrectInfo.message}
                          </p>
                        )}
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => {
                            setShowCustomDelegation(false);
                            setCustomDelegation("");
                            setAutoCorrectInfo(null);
                          }}
                        >
                          ← Back to list
                        </Button>
                      </div>
                    )}
                  </div>
                </div>

                {/* ROOMS & SURFACE */}
                <div className="grid grid-cols-2 gap-4">
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
                    <Label>Surface (m²) *</Label>
                    <Input
                      type="number"
                      value={form.surface}
                      onChange={(e) =>
                        setForm((f) => ({ ...f, surface: e.target.value }))
                      }
                      placeholder="120"
                      className={
                        !form.surface || parseFloat(form.surface) <= 0
                          ? "border-red-500"
                          : ""
                      }
                    />
                  </div>
                </div>
              </div>

              {/* MAP SECTION */}
              <div className="space-y-2 pt-4 border-t">
                <Label className="flex items-center gap-2">
                  <MapPin className="h-4 w-4" /> Exact Location *
                </Label>

                <ClientMap
                  ref={mapRef}
                  initialCenter={[36.8065, 10.1815]}
                  initialZoom={12}
                  height="350px"
                  onLocationSelect={handleLocationSelect}
                />

                {form.latitude && form.longitude ? (
                  <div className="flex gap-4 text-xs text-green-600 bg-green-50 p-2 rounded">
                    <CheckCircle2 className="h-3 w-3" />
                    <span>Lat: {form.latitude.toFixed(6)}</span>
                    <span>Lng: {form.longitude.toFixed(6)}</span>
                  </div>
                ) : (
                  <p className="text-xs text-red-500">
                    Click on the map to set property location
                  </p>
                )}
              </div>

              {/* Validation Summary */}
              {!step1Valid && (
                <div className="mt-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
                  <p className="text-sm font-medium text-yellow-800">
                    Please fix the following before continuing:
                  </p>
                  <ul className="text-xs text-yellow-700 mt-1 list-disc list-inside">
                    {(!form.title || titleValid === false) && (
                      <li>Valid title is required (min 5 chars, no test words)</li>
                    )}
                    {!form.type && <li>Property type is required</li>}
                    {!form.transaction && <li>Transaction type is required</li>}
                    {!selectedGovernorateId && <li>Governorate is required</li>}
                    {!selectedDelegationId && !(showCustomDelegation && customDelegation) && (
                      <li>Delegation area is required</li>
                    )}
                    {(!form.surface || parseFloat(form.surface) <= 0) && (
                      <li>Valid surface area is required</li>
                    )}
                    {(!form.latitude || !form.longitude) && (
                      <li>Click on map to set property location</li>
                    )}
                  </ul>
                </div>
              )}
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
              {/* Image Upload Section */}
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
                  className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors cursor-pointer hover:border-primary/50 hover:bg-accent/50 ${
                    uploading ? "opacity-50 pointer-events-none" : ""
                  }`}
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
                    disabled={generating || form.images.length === 0 || !form.city}
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
                Our AI model estimates a fair market price based on your property
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
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
                        Max <span className="text-red-500 font-bold">(ceiling)</span>
                      </p>
                      <p className="text-sm font-medium text-red-600">
                        {prediction.price_high.toLocaleString()}
                      </p>
                    </div>
                  </div>

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
                            100
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
                  className={`text-lg ${
                    isBeyondCeiling
                      ? "border-red-500 focus-visible:ring-red-500"
                      : ""
                  }`}
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
                  { label: "Governorate", value: selectedGovernorateName || "—" },
                  {
                    label: "Delegation",
                    value: showCustomDelegation
                      ? customDelegation || "—"
                      : delegations.find((d) => d.id === selectedDelegationId)?.name || "—",
                  },
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
                        ? `${form.latitude.toFixed(4)}, ${form.longitude.toFixed(4)}`
                        : "Not set",
                  },
                ].map((item) => (
                  <div key={item.label}>
                    <p className="text-xs text-muted-foreground">{item.label}</p>
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
                        alt="Preview"
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
              disabled={step === 0 ? !step1Valid : false}
            >
              Next <ArrowRight className="h-4 w-4 ml-2" />
            </Button>
          ) : (
            <Button
              onClick={publishListing}
              disabled={publishing || !form.title || !form.city || !form.surface}
            >
              {publishing ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" /> Publishing...
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