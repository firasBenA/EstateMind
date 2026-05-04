/**
 * frontend-client/src/pages/dashboard/FraudDashboard.tsx
 * Dashboard principal avec toggle entre image et texte
 */
import { useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Image as ImageIcon, FileText } from "lucide-react";
import FraudCenter from "./FraudCenter";
import TextFraudCenter from "./TextFraudeCenter";
import { DashboardLayout } from "@/components/DashboardLayout";

export default function FraudDashboard() {
  const [activeTab, setActiveTab] = useState<"images" | "text">("images");

  return (
    <DashboardLayout>
        <div className="space-y-4">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold">Fraud Detection Center</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Analyse multi-modale des annonces suspectes
        </p>
      </div>

      {/* Tabs - TabsList doit être à l'intérieur de Tabs */}
      <Tabs 
        value={activeTab} 
        onValueChange={(v) => setActiveTab(v as "images" | "text")}
        className="w-full"
      >
        <div className="flex items-center justify-between mb-4">
          <div /> {/* espace vide pour l'alignement */}
          <TabsList className="grid w-[300px] grid-cols-2">
            <TabsTrigger value="images" className="gap-2">
              <ImageIcon className="h-4 w-4" />
              Multimodal
            </TabsTrigger>
            <TabsTrigger value="text" className="gap-2">
              <FileText className="h-4 w-4" />
              Textuelle
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="images" className="mt-4">
          <FraudCenter />
        </TabsContent>

        <TabsContent value="text" className="mt-4">
          <TextFraudCenter />
        </TabsContent>
      </Tabs>
    </div>
    </DashboardLayout>
    
  );
}