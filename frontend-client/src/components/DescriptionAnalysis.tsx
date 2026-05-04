/**
 * frontend-client/src/components/DescriptionAnalysis.tsx
 * Analyse IA de la description - Version professionnelle
 */
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  AlertTriangle,
  ShieldCheck,
  ShieldAlert,
  MessageSquare,
  Flag,
  Brain,
  AlertCircle,
  CheckCircle2,
  XCircle,
} from "lucide-react";

interface DescriptionAnalysisProps {
  listingId: string;
}

interface AnalysisData {
  score_final: number;
  label_final: string;
  warning?: string;
  sentiment_stars?: number;
  sentiment_score?: number;
  zeroshot_label?: string;
  zeroshot_score?: number;
  rules_details?: string;
  rules_count?: number;
  nb_emojis?: number;
  confiance?: string;
  skipped?: boolean;
}

export function DescriptionAnalysis({ listingId }: DescriptionAnalysisProps) {
  const [analysis, setAnalysis] = useState<AnalysisData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchAnalysis = async () => {
      if (!listingId) {
        setLoading(false);
        return;
      }
      
      try {
        setError(null);
        const response = await fetch(`/api/listing/fraud-description/${listingId}/`, {
          credentials: "include",
        });
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
          setAnalysis(data.data);
        } else {
          setError(data.error || "Analyse non disponible");
        }
      } catch (error) {
        console.error("Error fetching description analysis:", error);
        setError("Impossible de charger l'analyse");
      } finally {
        setLoading(false);
      }
    };

    fetchAnalysis();
  }, [listingId]);

  if (loading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-4 w-32" />
        <Skeleton className="h-20 w-full" />
        <div className="grid grid-cols-2 gap-3">
          <Skeleton className="h-16 w-full" />
          <Skeleton className="h-16 w-full" />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-center">
        <AlertCircle className="h-8 w-8 mx-auto mb-2 text-red-500" />
        <p className="text-sm text-red-600">{error}</p>
      </div>
    );
  }

  if (!analysis || analysis.skipped) {
    return (
      <div className="rounded-lg border border-gray-200 bg-gray-50 p-4 text-center">
        <MessageSquare className="h-8 w-8 mx-auto mb-2 text-gray-400" />
        <p className="text-sm text-gray-600">Description trop courte pour analyse</p>
        <p className="text-xs text-gray-400 mt-1">Minimum 10 mots requis</p>
      </div>
    );
  }

  const getRiskConfig = (label: string) => {
    const configs = {
      negatif: { icon: XCircle, color: "red", bg: "bg-red-50", border: "border-red-200", text: "Très suspect" },
      neutre_negatif: { icon: AlertTriangle, color: "orange", bg: "bg-orange-50", border: "border-orange-200", text: "Suspect" },
      neutre_positif: { icon: ShieldCheck, color: "blue", bg: "bg-blue-50", border: "border-blue-200", text: "Modéré" },
      positif: { icon: CheckCircle2, color: "green", bg: "bg-green-50", border: "border-green-200", text: "Fiable" },
    };
    return configs[label as keyof typeof configs] || configs.neutre_positif;
  };

  const riskConfig = getRiskConfig(analysis.label_final);
  const RiskIcon = riskConfig.icon;
  const scorePercent = Math.round(analysis.score_final * 100);

  return (
    <div className="space-y-4">
      {/* En-tête avec score */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Brain className="h-4 w-4 text-gray-500" />
          <span className="text-sm font-medium text-gray-700">Analyse du texte</span>
        </div>
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-2 cursor-help">
                <div className="text-right">
                  <div className="text-lg font-bold">{scorePercent}%</div>
                  <div className="text-xs text-gray-500">risque détecté</div>
                </div>
                <div className={`rounded-full p-1 ${riskConfig.bg}`}>
                  <RiskIcon className={`h-4 w-4 text-${riskConfig.color}-600`} />
                </div>
              </div>
            </TooltipTrigger>
            <TooltipContent>
              <p className="text-xs">Score basé sur l'analyse du contenu</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </div>

      {/* Barre de progression */}
      <Progress 
        value={scorePercent} 
        className="h-1.5"
        style={{
          backgroundColor: "#e5e7eb",
        }}
      />
      
      {/* Badge de statut */}
      <div className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium ${riskConfig.bg} ${riskConfig.text}`}>
        <RiskIcon className="h-3 w-3" />
        {riskConfig.text}
      </div>

      {/* Détails techniques */}
      <div className="grid grid-cols-1 gap-3 pt-2">
        {analysis.zeroshot_label && (
          <div className="rounded-lg border border-gray-100 bg-gray-50/50 p-2">
            <div className="flex items-center gap-1.5 mb-1">
              <Brain className="h-3 w-3 text-purple-500" />
              <span className="text-xs text-gray-500">Classification</span>
            </div>
            <div className="flex items-center justify-between">
              <Badge variant="outline" className="text-xs font-normal">
                {analysis.zeroshot_label}
              </Badge>
              <span className="text-xs text-gray-400">
                {Math.round((analysis.zeroshot_score || 0) * 100)}%
              </span>
            </div>
          </div>
        )}
      </div>

      {/* Règles détectées */}
      {analysis.rules_details && analysis.rules_details !== "aucun" && (
        <div className="rounded-lg border border-amber-100 bg-amber-50/30 p-2.5">
          <div className="flex items-center gap-2 mb-1.5">
            <Flag className="h-3.5 w-3.5 text-amber-600" />
            <span className="text-xs font-medium text-amber-700">
              Signaux ({analysis.rules_count})
            </span>
          </div>
          <div className="flex flex-wrap gap-1">
            {analysis.rules_details.split("|").map((rule: string, idx: number) => (
              <span key={idx} className="text-xs text-amber-700 bg-amber-100/50 px-1.5 py-0.5 rounded">
                {rule.replace(/_/g, " ")}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Métadonnées */}
      <div className="flex justify-between text-xs text-gray-400 pt-1 border-t border-gray-100">
        <span>Émojis: {analysis.nb_emojis || 0}</span>
        <span>Confiance: {analysis.confiance || "N/A"}</span>
      </div>
    </div>
  );
}