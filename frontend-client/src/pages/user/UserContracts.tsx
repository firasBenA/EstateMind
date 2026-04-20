// frontend-client/src/pages/user/UserContracts.tsx - Updated

import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Skeleton } from "@/components/ui/skeleton";
import { 
  FileSignature, Loader2, Download, Send, CheckCircle2, 
  Building2, User, Calendar, AlertCircle,
  Copy, Save, Eye, Edit3, Home, Search, Plus
} from "lucide-react";
import { useState, useRef, useEffect } from "react";
import { toast } from "sonner";
import { useAuth } from "@/lib/auth-context";
import { useListings } from "@/hooks/useListings";

interface ContractParams {
  contract_type: string;
  seller_name: string;
  seller_cin: string;
  seller_address: string;
  buyer_name: string;
  buyer_cin: string;
  buyer_address: string;
  listing_id: string;
  listing_title: string;
  listing_address: string;
  surface: number;
  price: number;
  transaction_date: string;
  transaction_type: string;
}

interface SavedContract {
  id: number;
  title: string;
  contract_type: string;
  buyer_name: string;
  created_at: string;
  status: "draft" | "sent" | "signed";
}

const contractTypes = [
  { value: "compromis_de_vente", label: "Sales Agreement (Compromis de Vente)" },
  { value: "promesse_de_vente", label: "Promise to Sell (Promesse de Vente)" },
  { value: "contrat_de_location", label: "Rental Agreement (Contrat de Location)" },
  { value: "acte_de_vente", label: "Final Deed of Sale (Acte de Vente)" },
];

export default function UserContracts() {
  const { user } = useAuth();
  const [activeTab, setActiveTab] = useState("generate");
  const [generating, setGenerating] = useState(false);
  const [saving, setSaving] = useState(false);
  const [contractText, setContractText] = useState("");
  const [isEditing, setIsEditing] = useState(false);
  const [streamError, setStreamError] = useState("");
  const [generatedContractId, setGeneratedContractId] = useState<number | null>(null);
  const [selectedListing, setSelectedListing] = useState<any>(null);
  const [loadingListing, setLoadingListing] = useState(false);
  const [savedContracts, setSavedContracts] = useState<SavedContract[]>([]);
  const [searchMode, setSearchMode] = useState<"my_listings" | "by_id">("my_listings");
  
  // Fetch user's own listings
  const { listings: userListings, loading: loadingUserListings } = useListings({ 
    user_id: user?.id?.toString() // Adjust based on your auth system
  });
  
  // Form state
  const [contractType, setContractType] = useState("compromis_de_vente");
  const [sellerName, setSellerName] = useState(user?.name || user?.username || "");
  const [sellerCin, setSellerCin] = useState("");
  const [sellerAddress, setSellerAddress] = useState("");
  const [buyerName, setBuyerName] = useState("");
  const [buyerCin, setBuyerCin] = useState("");
  const [buyerAddress, setBuyerAddress] = useState("");
  const [transactionDate, setTransactionDate] = useState(new Date().toISOString().split('T')[0]);
  const [manualListingId, setManualListingId] = useState("");
  
  const contractRef = useRef<HTMLDivElement>(null);

  // Fetch saved contracts
  useEffect(() => {
    fetch("/api/contracts/", { credentials: "include" })
      .then(r => r.ok ? r.json() : { contracts: [] })
      .then(d => setSavedContracts(d.contracts ?? []))
      .catch(() => {});
  }, [generatedContractId]);

  // Handle listing selection from user's listings
  const handleSelectListing = (listing: any) => {
    setSelectedListing(listing);
    // Auto-fill seller info from user profile if not set
    if (!sellerName && user) {
      setSellerName(user.name || user.username || "");
    }
    toast.success(`Selected: ${listing.title}`);
  };

  // Handle manual listing ID search
  const handleSearchById = async () => {
    if (!manualListingId) {
      toast.error("Please enter a listing ID");
      return;
    }
    
    setLoadingListing(true);
    try {
      const resp = await fetch(`/api/contracts/listing/${manualListingId}/`, {
        credentials: "include",
      });
      
      if (!resp.ok) throw new Error("Listing not found");
      
      const data = await resp.json();
      setSelectedListing(data.listing);
      toast.success("Listing loaded successfully");
    } catch (error) {
      toast.error("Listing not found. Please check the ID.");
      setSelectedListing(null);
    } finally {
      setLoadingListing(false);
    }
  };

  // Generate contract
  const handleGenerate = async () => {
    if (!buyerName || !buyerCin) {
      toast.error("Please fill in buyer details");
      return;
    }
    
    if (!selectedListing) {
      toast.error("Please select a listing first");
      return;
    }
    
    setGenerating(true);
    setContractText("");
    setStreamError("");
    setGeneratedContractId(null);
    
    const params: ContractParams = {
      contract_type: contractType,
      seller_name: sellerName,
      seller_cin: sellerCin,
      seller_address: sellerAddress,
      buyer_name: buyerName,
      buyer_cin: buyerCin,
      buyer_address: buyerAddress,
      listing_id: selectedListing.id,
      listing_title: selectedListing.title,
      listing_address: `${selectedListing.city || ""} ${selectedListing.municipality || ""} ${selectedListing.zone || ""}`.trim(),
      surface: selectedListing.surface || 0,
      price: selectedListing.price || 0,
      transaction_date: transactionDate,
      transaction_type: selectedListing.transaction_type || "sale",
    };
    
    try {
      const resp = await fetch("/api/contracts/generate/", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ contract_type: contractType, params }),
      });
      
      if (!resp.ok || !resp.body) {
        throw new Error(`HTTP ${resp.status}`);
      }
      
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n\n");
        buffer = lines.pop() ?? "";
        
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const evt = JSON.parse(line.slice(6));
            if (evt.error) {
              setStreamError(evt.error);
              break;
            }
            if (evt.token) {
              setContractText(prev => prev + evt.token);
            }
          } catch { /* malformed line */ }
        }
      }
    } catch (error) {
      setStreamError(error instanceof Error ? error.message : "Generation failed");
    } finally {
      setGenerating(false);
      setTimeout(() => contractRef.current?.scrollIntoView({ behavior: "smooth" }), 100);
    }
  };
  
  // Save contract
  const handleSave = async () => {
    if (!contractText) return;
    
    setSaving(true);
    const title = `${contractType.replace(/_/g, " ")} - ${selectedListing?.title || "Property"} - ${new Date().toLocaleDateString()}`;
    
    try {
      const resp = await fetch("/api/contracts/save/", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contract_type: contractType,
          title,
          params: {
            buyer_name: buyerName,
            listing_id: selectedListing?.id,
          },
          content: contractText,
        }),
      });
      
      const data = await resp.json();
      if (resp.ok) {
        setGeneratedContractId(data.id);
        toast.success("Contract saved successfully");
      } else {
        toast.error(data.error ?? "Failed to save");
      }
    } catch {
      toast.error("Network error");
    } finally {
      setSaving(false);
    }
  };
  
  // Copy to clipboard
  const handleCopy = () => {
    navigator.clipboard.writeText(contractText);
    toast.success("Copied to clipboard");
  };
  
  // Export PDF
  const handleExportPDF = async () => {
    if (!generatedContractId) {
      toast.error("Please save the contract before exporting");
      return;
    }
    window.open(`/api/contracts/${generatedContractId}/pdf/`, "_blank");
  };
  
  // Send for signature
  const handleSendForSignature = async () => {
    if (!generatedContractId) {
      toast.error("Please save the contract first");
      return;
    }
    
    try {
      const resp = await fetch(`/api/contracts/${generatedContractId}/send/`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: buyerName }),
      });
      
      if (resp.ok) {
        toast.success(`Contract sent to ${buyerName} for signature`);
      } else {
        toast.error("Failed to send contract");
      }
    } catch {
      toast.error("Network error");
    }
  };
  
  // Markdown renderer
  const MarkdownBlock = ({ text }: { text: string }) => {
    const lines = text.split("\n");
    return (
      <div className="space-y-1.5 font-mono text-[13px] leading-relaxed">
        {lines.map((line, i) => {
          if (line.startsWith("ARTICLE ")) {
            return <p key={i} className="text-base font-bold text-foreground mt-4 mb-2">{line}</p>;
          }
          if (line.startsWith("- ") || line.startsWith("* ")) {
            return <p key={i} className="text-muted-foreground pl-4">• {line.slice(2)}</p>;
          }
          if (line.trim() === "") return <div key={i} className="h-2" />;
          return <p key={i} className="text-muted-foreground">{line}</p>;
        })}
      </div>
    );
  };
  
  return (
    <UserDashboardLayout>
      <div className="space-y-8">
        <div>
          <h1 className="text-2xl font-bold">Contracts</h1>
          <p className="text-muted-foreground">Generate legally compliant contracts auto-filled with your listing data</p>
        </div>
        
        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList>
            <TabsTrigger value="generate">Generate Contract</TabsTrigger>
            <TabsTrigger value="history">Contract History</TabsTrigger>
          </TabsList>
          
          <TabsContent value="generate" className="space-y-6">
            {/* Step 1: Select Listing */}
            <Card>
              <CardHeader>
                <CardTitle>1. Select Property Listing</CardTitle>
                <CardDescription>Choose a listing from your properties or search by ID</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex gap-2 mb-4">
                  <Button 
                    variant={searchMode === "my_listings" ? "default" : "outline"}
                    onClick={() => setSearchMode("my_listings")}
                    className="flex-1"
                  >
                    <Home className="h-4 w-4 mr-2" />
                    My Listings
                  </Button>
                  <Button 
                    variant={searchMode === "by_id" ? "default" : "outline"}
                    onClick={() => setSearchMode("by_id")}
                    className="flex-1"
                  >
                    <Search className="h-4 w-4 mr-2" />
                    Search by ID
                  </Button>
                </div>
                
                {searchMode === "my_listings" && (
                  <div className="space-y-3">
                    {loadingUserListings ? (
                      <div className="space-y-2">
                        <Skeleton className="h-20 w-full" />
                        <Skeleton className="h-20 w-full" />
                      </div>
                    ) : userListings && userListings.length > 0 ? (
                      <div className="max-h-64 overflow-y-auto space-y-2">
                        {userListings.map((listing) => (
                          <div
                            key={listing.id}
                            onClick={() => handleSelectListing(listing)}
                            className={`p-3 rounded-lg border cursor-pointer transition-all ${
                              selectedListing?.id === listing.id
                                ? "border-primary bg-primary/5"
                                : "hover:border-border hover:bg-muted/30"
                            }`}
                          >
                            <p className="font-medium text-sm">{listing.title}</p>
                            <p className="text-xs text-muted-foreground mt-1">
                              {listing.city} • {listing.surface} m² • {listing.price?.toLocaleString()} TND
                            </p>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-center py-8">
                        <Building2 className="h-12 w-12 mx-auto text-muted-foreground mb-3" />
                        <p className="text-sm text-muted-foreground">You don't have any listings yet</p>
                        <Button variant="outline" size="sm" className="mt-2" asChild>
                          <a href="/user/listings/create">
                            <Plus className="h-4 w-4 mr-2" />
                            Create a Listing
                          </a>
                        </Button>
                      </div>
                    )}
                  </div>
                )}
                
                {searchMode === "by_id" && (
                  <div className="space-y-4">
                    <div className="flex gap-4">
                      <Input
                        placeholder="Enter listing ID (e.g., century21:12345)"
                        value={manualListingId}
                        onChange={(e) => setManualListingId(e.target.value)}
                        className="flex-1"
                      />
                      <Button onClick={handleSearchById} disabled={loadingListing}>
                        {loadingListing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
                        <span className="ml-2">Load</span>
                      </Button>
                    </div>
                    
                    {selectedListing && searchMode === "by_id" && (
                      <div className="p-4 bg-muted/30 rounded-lg">
                        <p className="font-semibold">{selectedListing.title}</p>
                        <p className="text-sm text-muted-foreground mt-1">
                          {selectedListing.city} • {selectedListing.surface} m² • {selectedListing.price?.toLocaleString()} TND
                        </p>
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
            
            {/* Step 2: Fill Party Details */}
            <Card>
              <CardHeader>
                <CardTitle>2. Party Details</CardTitle>
                <CardDescription>Seller and buyer information for the contract</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-4">
                  <h3 className="text-sm font-semibold flex items-center gap-2">
                    <User className="h-4 w-4" /> Seller Information
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label>Full Name</Label>
                      <Input value={sellerName} onChange={(e) => setSellerName(e.target.value)} />
                    </div>
                    <div>
                      <Label>CIN / National ID</Label>
                      <Input value={sellerCin} onChange={(e) => setSellerCin(e.target.value)} />
                    </div>
                    <div className="md:col-span-2">
                      <Label>Address</Label>
                      <Textarea value={sellerAddress} onChange={(e) => setSellerAddress(e.target.value)} rows={2} />
                    </div>
                  </div>
                </div>
                
                <div className="space-y-4">
                  <h3 className="text-sm font-semibold flex items-center gap-2">
                    <User className="h-4 w-4" /> Buyer Information
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label>Full Name *</Label>
                      <Input value={buyerName} onChange={(e) => setBuyerName(e.target.value)} required />
                    </div>
                    <div>
                      <Label>CIN / National ID *</Label>
                      <Input value={buyerCin} onChange={(e) => setBuyerCin(e.target.value)} required />
                    </div>
                    <div className="md:col-span-2">
                      <Label>Address</Label>
                      <Textarea value={buyerAddress} onChange={(e) => setBuyerAddress(e.target.value)} rows={2} />
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
            
            {/* Step 3: Contract Configuration */}
            <Card>
              <CardHeader>
                <CardTitle>3. Contract Configuration</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <Label>Contract Type</Label>
                    <Select value={contractType} onValueChange={setContractType}>
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {contractTypes.map(t => (
                          <SelectItem key={t.value} value={t.value}>{t.label}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label>Transaction Date</Label>
                    <Input type="date" value={transactionDate} onChange={(e) => setTransactionDate(e.target.value)} />
                  </div>
                </div>
                
                <Button onClick={handleGenerate} disabled={generating || !selectedListing} className="w-full gap-2" size="lg">
                  {generating ? (
                    <><Loader2 className="h-4 w-4 animate-spin" /> Generating Contract...</>
                  ) : (
                    <><FileSignature className="h-4 w-4" /> Generate Contract</>
                  )}
                </Button>
              </CardContent>
            </Card>
            
            {/* Step 4: Generated Contract */}
            {(generating || contractText || streamError) && (
              <div ref={contractRef}>
                <div className="flex items-center justify-between mb-3">
                  <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                    4. Generated Contract
                  </p>
                  {contractText && !generating && (
                    <div className="flex gap-2">
                      <Button variant="outline" size="sm" onClick={() => setIsEditing(!isEditing)} className="gap-1.5">
                        {isEditing ? <Eye className="h-3.5 w-3.5" /> : <Edit3 className="h-3.5 w-3.5" />}
                        {isEditing ? "Preview" : "Edit"}
                      </Button>
                      <Button variant="outline" size="sm" onClick={handleCopy} className="gap-1.5">
                        <Copy className="h-3.5 w-3.5" /> Copy
                      </Button>
                      <Button variant="outline" size="sm" onClick={handleSave} disabled={saving} className="gap-1.5">
                        <Save className="h-3.5 w-3.5" />
                        {saving ? "Saving..." : "Save"}
                      </Button>
                      <Button variant="outline" size="sm" onClick={handleExportPDF} className="gap-1.5">
                        <Download className="h-3.5 w-3.5" /> PDF
                      </Button>
                      <Button size="sm" onClick={handleSendForSignature} className="gap-1.5">
                        <Send className="h-3.5 w-3.5" /> Send for Signature
                      </Button>
                    </div>
                  )}
                </div>
                
                <Card>
                  <CardContent className="pt-5">
                    {streamError && (
                      <div className="flex items-start gap-2 text-destructive text-sm">
                        <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
                        <div>
                          <p className="font-medium">Generation failed</p>
                          <p className="text-xs mt-0.5 text-muted-foreground">{streamError}</p>
                        </div>
                      </div>
                    )}
                    
                    {!streamError && (
                      <div className="min-h-[400px]">
                        {generating && !contractText && (
                          <div className="space-y-3">
                            <Skeleton className="h-4 w-1/3" />
                            <Skeleton className="h-3 w-full" />
                            <Skeleton className="h-3 w-5/6" />
                            <Skeleton className="h-3 w-4/5" />
                          </div>
                        )}
                        
                        {contractText && (
                          <>
                            {isEditing ? (
                              <Textarea
                                value={contractText}
                                onChange={(e) => setContractText(e.target.value)}
                                className="w-full min-h-[500px] p-4 font-mono text-sm"
                              />
                            ) : (
                              <MarkdownBlock text={contractText} />
                            )}
                          </>
                        )}
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
            )}
          </TabsContent>
          
          <TabsContent value="history">
            <Card>
              <CardHeader>
                <CardTitle>Contract History</CardTitle>
              </CardHeader>
              <CardContent>
                {savedContracts.length === 0 ? (
                  <div className="text-center py-12">
                    <FileSignature className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
                    <p className="text-muted-foreground">No contracts generated yet</p>
                    <Button variant="outline" className="mt-4" onClick={() => setActiveTab("generate")}>
                      Generate your first contract
                    </Button>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {savedContracts.map(contract => (
                      <Card key={contract.id}>
                        <CardContent className="flex items-center justify-between py-4">
                          <div className="flex items-center gap-4">
                            <FileSignature className="h-8 w-8 text-muted-foreground" />
                            <div>
                              <p className="font-medium text-sm">{contract.title}</p>
                              <p className="text-xs text-muted-foreground">
                                With {contract.buyer_name} • {new Date(contract.created_at).toLocaleDateString()}
                              </p>
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            <Badge variant={contract.status === "signed" ? "default" : "secondary"}>
                              {contract.status === "signed" ? (
                                <><CheckCircle2 className="h-3 w-3 mr-1" /> Signed</>
                              ) : contract.status === "sent" ? "Sent" : "Draft"}
                            </Badge>
                            <Button variant="outline" size="sm">
                              <Download className="h-4 w-4" />
                            </Button>
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </UserDashboardLayout>
  );
}