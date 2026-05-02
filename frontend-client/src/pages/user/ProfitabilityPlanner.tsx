"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Slider } from "@/components/ui/slider";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  LineChart,
  Line,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ComposedChart,
} from "recharts";
import {
  TrendingUp,
  BarChart3,
  Download,
  Copy,
  Check,
  Loader2,
} from "lucide-react";

import { predictionApi, locationsApi, ScenarioInput, PredictionResult, Governorate, Delegation } from "@/lib/api";

const propertyTypes = [
  { value: "Apartment", label: "Appartement" },
  { value: "Villa", label: "Villa" },
  { value: "Land", label: "Terrain" },
  { value: "Commercial", label: "Commercial" },
];

export default function ScenarioBuilderPage() {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<PredictionResult | null>(null);
  const [selectedYear, setSelectedYear] = useState<number | null>(null);
  const [copied, setCopied] = useState(false);
  const [activeTab, setActiveTab] = useState("chart");
  
  // Locations state
  const [governorates, setGovernorates] = useState<Governorate[]>([]);
  const [delegations, setDelegations] = useState<Delegation[]>([]);
  const [loadingLocations, setLoadingLocations] = useState(true);
  
  const [formData, setFormData] = useState<ScenarioInput>({
    property_type: "Apartment",
    surface: 100,
    city: "",
    region: "",
    years: 10,
    monthly_rent: undefined,
  });

  // Charger les gouvernorats au montage
  useEffect(() => {
    const loadGovernorates = async () => {
      try {
        const data = await locationsApi.getGovernorates();
        setGovernorates(data);
        if (data.length > 0) {
          // Sélectionner le premier gouvernorat par défaut
          const firstGov = data[0];
          setFormData(prev => ({ ...prev, region: firstGov.name, city: "" }));
          // Charger ses délégations
          const deps = await locationsApi.getDelegations(firstGov.id);
          setDelegations(deps);
        }
      } catch (error) {
        console.error("Failed to load governorates:", error);
      } finally {
        setLoadingLocations(false);
      }
    };
    loadGovernorates();
  }, []);

  // Charger les délégations quand le gouvernorat change
  const handleGovernorateChange = async (governorateId: number, governorateName: string) => {
    setFormData(prev => ({ ...prev, region: governorateName, city: "" }));
    try {
      const deps = await locationsApi.getDelegations(governorateId);
      setDelegations(deps);
    } catch (error) {
      console.error("Failed to load delegations:", error);
      setDelegations([]);
    }
  };

  const handlePredict = async () => {
    setLoading(true);
    setResult(null);
    
    try {
      const response = await predictionApi.predict(formData);
      
      if (response.success && response.data) {
        setResult(response.data);
        setSelectedYear(response.data.yearly_predictions.length - 1);
      }
    } catch (error) {
      console.error("Prediction error:", error);
    } finally {
      setLoading(false);
    }
  };

  const chartData = result?.yearly_predictions.map(p => ({
    year: p.year,
    price: p.price / 1000,
    rent: p.cumulative_rent / 1000,
    total: p.total_value / 1000,
    roi: p.roi,
  })) || [];

  const selectedData = selectedYear !== null && result
    ? result.yearly_predictions.find(p => p.year === selectedYear)
    : null;

  const handleCopyResults = () => {
    if (!result) return;
    const text = `Simulation Investissement Immobilier
Prix initial: ${(result.initial_price / 1000).toFixed(0)}k TND
Valeur finale: ${(result.final_value / 1000).toFixed(0)}k TND
ROI total: +${result.total_roi}%
Horizon: ${formData.years} ans
Type: ${formData.property_type}
Ville: ${formData.city || formData.region}`;
    
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const formatCurrency = (value: number) => {
    if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M DT`;
    if (value >= 1_000) return `${(value / 1_000).toFixed(0)}k DT`;
    return `${value} DT`;
  };

  return (
    <UserDashboardLayout>
      <div className="max-w-6xl mx-auto space-y-6 pb-12">
        {/* Header */}
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight">Simulateur d'investissement</h1>
          <p className="text-sm text-muted-foreground">
            Projection sur la base des modèles macroéconomiques
          </p>
        </div>

        <div className="grid lg:grid-cols-12 gap-6">
          {/* Left Panel - Input Form */}
          <div className="lg:col-span-5">
            <Card>
              <CardContent className="p-5 space-y-5">
                {/* Property Type */}
                <div className="space-y-2">
                  <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                    Type de bien
                  </Label>
                  <div className="grid grid-cols-2 gap-2">
                    {propertyTypes.map((type) => (
                      <Button
                        key={type.value}
                        type="button"
                        variant={formData.property_type === type.value ? "default" : "outline"}
                        size="sm"
                        className="justify-start text-sm font-normal"
                        onClick={() => setFormData({ ...formData, property_type: type.value as any })}
                      >
                        {type.label}
                      </Button>
                    ))}
                  </div>
                </div>

                {/* Surface */}
                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <Label className="text-muted-foreground">Surface</Label>
                    <span className="text-foreground font-medium">{formData.surface} m²</span>
                  </div>
                  <Slider
                    value={[formData.surface]}
                    onValueChange={([val]) => setFormData({ ...formData, surface: val })}
                    min={30}
                    max={500}
                    step={5}
                  />
                </div>

                {/* Location - Dynamic from Database */}
                <div className="grid grid-cols-2 gap-3">
                  <div className="space-y-1">
                    <Label className="text-xs text-muted-foreground">Gouvernorat</Label>
                    <Select
                      value={formData.region}
                      onValueChange={(val) => {
                        const selected = governorates.find(g => g.name === val);
                        if (selected) {
                          handleGovernorateChange(selected.id, selected.name);
                        }
                      }}
                      disabled={loadingLocations}
                    >
                      <SelectTrigger className="h-9">
                        <SelectValue placeholder={loadingLocations ? "Chargement..." : "Sélectionner"}>
                          {formData.region || (loadingLocations ? "Chargement..." : "Sélectionner un gouvernorat")}
                        </SelectValue>
                      </SelectTrigger>
                      <SelectContent>
                        {governorates.map((gov) => (
                          <SelectItem key={gov.id} value={gov.name}>
                            {gov.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs text-muted-foreground">Délégation / Ville</Label>
                    <Select
                      value={formData.city}
                      onValueChange={(val) => setFormData({ ...formData, city: val })}
                      disabled={!formData.region || delegations.length === 0}
                    >
                      <SelectTrigger className="h-9">
                        <SelectValue placeholder="Sélectionner" />
                      </SelectTrigger>
                      <SelectContent>
                        {delegations.map((del) => (
                          <SelectItem key={del.id} value={del.name}>
                            {del.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {/* Horizon */}
                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <Label className="text-muted-foreground">Horizon</Label>
                    <span className="text-foreground font-medium">{formData.years} ans</span>
                  </div>
                  <Slider
                    value={[formData.years]}
                    onValueChange={([val]) => setFormData({ ...formData, years: val })}
                    min={1}
                    max={25}
                    step={1}
                  />
                </div>

                {/* Rental Income */}
                <div className="space-y-1">
                  <Label className="text-xs text-muted-foreground">Loyer mensuel (optionnel)</Label>
                  <Input
                    type="number"
                    placeholder="0"
                    value={formData.monthly_rent || ""}
                    onChange={(e) => setFormData({ 
                      ...formData, 
                      monthly_rent: e.target.value ? parseFloat(e.target.value) : undefined 
                    })}
                    className="h-9"
                  />
                </div>

                {/* Predict Button */}
                <Button
                  onClick={handlePredict}
                  disabled={loading || !formData.region}
                  className="w-full mt-2"
                  size="default"
                >
                  {loading ? (
                    <span className="flex items-center gap-2">
                      <Loader2 className="w-4 h-4 animate-spin" />
                      Calcul en cours
                    </span>
                  ) : (
                    "Lancer la simulation"
                  )}
                </Button>
              </CardContent>
            </Card>
          </div>

          {/* Right Panel - Results */}
          <div className="lg:col-span-7">
            {result ? (
              <div className="space-y-5">
                {/* Key Metrics */}
                <div className="grid grid-cols-3 gap-3">
                  <div className="text-center p-3 rounded-lg bg-muted/20">
                    <p className="text-xs text-muted-foreground">Valeur initiale</p>
                    <p className="text-xl font-semibold">{formatCurrency(result.initial_price)}</p>
                  </div>
                  <div className="text-center p-3 rounded-lg bg-muted/20">
                    <p className="text-xs text-muted-foreground">Valeur finale</p>
                    <p className="text-xl font-semibold text-emerald-600 dark:text-emerald-400">
                      {formatCurrency(result.final_value)}
                    </p>
                  </div>
                  <div className="text-center p-3 rounded-lg bg-muted/20">
                    <p className="text-xs text-muted-foreground">ROI total</p>
                    <p className={`text-xl font-semibold ${result.total_roi >= 0 ? "text-emerald-600 dark:text-emerald-400" : "text-red-600"}`}>
                      {result.total_roi >= 0 ? "+" : ""}{Math.round(result.total_roi)}%
                    </p>
                  </div>
                </div>

                {/* Confidence */}
                <div className="space-y-1">
                  <div className="flex justify-between text-xs text-muted-foreground">
                    <span>Confiance du modèle</span>
                    <span>{Math.round(result.confidence_score)}%</span>
                  </div>
                  <div className="h-1 bg-muted rounded-full overflow-hidden">
                    <div
                      className="h-full bg-primary rounded-full transition-all"
                      style={{ width: `${result.confidence_score}%` }}
                    />
                  </div>
                </div>

                {/* Tabs */}
                <Tabs value={activeTab} onValueChange={setActiveTab}>
                  <TabsList className="grid w-full grid-cols-2 h-9">
                    <TabsTrigger value="chart" className="text-xs">Projection</TabsTrigger>
                    <TabsTrigger value="table" className="text-xs">Détails annuels</TabsTrigger>
                  </TabsList>
                  
                  <TabsContent value="chart" className="mt-4">
                    <Card>
                      <CardContent className="p-4">
                        <ResponsiveContainer width="100%" height={300}>
                          <ComposedChart data={chartData}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                            <XAxis dataKey="year" tick={{ fontSize: 11 }} />
                            <YAxis yAxisId="left" tick={{ fontSize: 11 }} />
                            <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11 }} />
                            <Tooltip
                              formatter={(value: number, name: string) => {
                                if (name === "roi") return [`${value.toFixed(1)}%`, "ROI"];
                                return [`${value.toFixed(0)}k DT`, name === "price" ? "Valeur" : "Revenus"];
                              }}
                            />
                            <Legend wrapperStyle={{ fontSize: 11 }} />
                            <Area
                              yAxisId="left"
                              type="monotone"
                              dataKey="price"
                              fill="#3b82f6"
                              stroke="#3b82f6"
                              name="Valeur du bien"
                              fillOpacity={0.1}
                            />
                            <Area
                              yAxisId="left"
                              type="monotone"
                              dataKey="rent"
                              fill="#10b981"
                              stroke="#10b981"
                              name="Revenus locatifs"
                              fillOpacity={0.1}
                            />
                            <Line
                              yAxisId="right"
                              type="monotone"
                              dataKey="roi"
                              stroke="#f59e0b"
                              name="ROI"
                              strokeWidth={1.5}
                              dot={false}
                            />
                          </ComposedChart>
                        </ResponsiveContainer>
                      </CardContent>
                    </Card>
                  </TabsContent>
                  
                  <TabsContent value="table" className="mt-4">
                    <Card>
                      <CardContent className="p-0">
                        <div className="overflow-x-auto">
                          <table className="w-full text-sm">
                            <thead className="border-b">
                              <tr className="text-left text-muted-foreground">
                                <th className="p-3 font-medium">Année</th>
                                <th className="p-3 text-right font-medium">Valeur</th>
                                <th className="p-3 text-right font-medium">Revenus</th>
                                <th className="p-3 text-right font-medium">Valeur totale</th>
                                <th className="p-3 text-right font-medium">ROI</th>
                                <th className="p-3 text-right font-medium">Inflation</th>
                              </tr>
                            </thead>
                            <tbody>
                              {result.yearly_predictions.map((p) => (
                                <tr
                                  key={p.year}
                                  className={`border-b cursor-pointer hover:bg-muted/30 transition-colors ${
                                    selectedYear === p.year ? "bg-primary/5" : ""
                                  }`}
                                  onClick={() => setSelectedYear(p.year)}
                                >
                                  <td className="p-3 font-mono text-xs">{p.year}</td>
                                  <td className="p-3 text-right">{formatCurrency(p.price)}</td>
                                  <td className="p-3 text-right text-emerald-600">{formatCurrency(p.cumulative_rent)}</td>
                                  <td className="p-3 text-right font-medium">{formatCurrency(p.total_value)}</td>
                                  <td className={`p-3 text-right ${p.roi >= 0 ? "text-emerald-600" : "text-red-600"}`}>
                                    {p.roi >= 0 ? "+" : ""}{p.roi}%
                                  </td>
                                  <td className="p-3 text-right text-muted-foreground">{p.inflation}%</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </CardContent>
                    </Card>
                  </TabsContent>
                </Tabs>

                {/* Year Selector */}
                <div className="flex gap-1 overflow-x-auto pb-1">
                  {result.yearly_predictions.map((p) => (
                    <button
                      key={p.year}
                      onClick={() => setSelectedYear(p.year)}
                      className={`px-3 py-1.5 rounded text-xs font-mono transition-all ${
                        selectedYear === p.year
                          ? "bg-primary text-primary-foreground"
                          : "bg-muted hover:bg-muted/80"
                      }`}
                    >
                      {p.year}
                    </button>
                  ))}
                </div>

                {/* Selected Year Details */}
                {selectedData && (
                  <div className="grid grid-cols-4 gap-3 text-center text-sm p-3 rounded-lg bg-muted/10">
                    <div>
                      <p className="text-xs text-muted-foreground">Valeur</p>
                      <p className="font-medium">{formatCurrency(selectedData.price)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Revenus</p>
                      <p className="font-medium text-emerald-600">{formatCurrency(selectedData.cumulative_rent)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Valeur totale</p>
                      <p className="font-semibold">{formatCurrency(selectedData.total_value)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">ROI</p>
                      <p className={`font-medium ${selectedData.roi >= 0 ? "text-emerald-600" : "text-red-600"}`}>
                        {selectedData.roi >= 0 ? "+" : ""}{selectedData.roi}%
                      </p>
                    </div>
                  </div>
                )}

                {/* Footer */}
                <div className="flex justify-between items-center pt-2 text-xs text-muted-foreground border-t">
                  <span>Modèle: {result.model_used}</span>
                  <div className="flex gap-2">
                    <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={handleCopyResults}>
                      {copied ? (
                        <><Check className="w-3 h-3 mr-1" />Copié</>
                      ) : (
                        <><Copy className="w-3 h-3 mr-1" />Copier</>
                      )}
                    </Button>
                    <Button variant="ghost" size="sm" className="h-7 text-xs">
                      <Download className="w-3 h-3 mr-1" />
                      Export
                    </Button>
                  </div>
                </div>
              </div>
            ) : (
              <Card className="h-full min-h-[400px] flex items-center justify-center">
                <CardContent className="text-center py-12">
                  <div className="w-12 h-12 mx-auto mb-3 rounded-full bg-muted flex items-center justify-center">
                    <TrendingUp className="w-6 h-6 text-muted-foreground" />
                  </div>
                  <h3 className="font-medium mb-1">Aucune simulation</h3>
                  <p className="text-sm text-muted-foreground">
                    Sélectionnez un gouvernorat et lancez une simulation
                  </p>
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>
    </UserDashboardLayout>
  );
}