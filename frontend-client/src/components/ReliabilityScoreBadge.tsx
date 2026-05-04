// frontend-client/src/components/ReliabilityScoreBadge.tsx

import { useState, useEffect } from "react";
import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Shield,
  ShieldAlert,
  ShieldCheck,
  AlertTriangle,
  Info,
} from "lucide-react";

interface FraudScore {
  score: number;
  risk_level: "incoherent" | "suspect" | "coherent";
  mismatch_types: string[];
  price_deviation: number;
  images_analyzed: number;
}

interface ReliabilityScoreBadgeProps {
  listingId: string;
  size?: "sm" | "md" | "lg";
  showLabel?: boolean;
}

export function ReliabilityScoreBadge({
  listingId,
  size = "md",
  showLabel = true,
}: ReliabilityScoreBadgeProps) {
  const [score, setScore] = useState<FraudScore | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchScore = async () => {
      try {
        const response = await fetch(`/api/listing/fraud-score/${listingId}/`, {
          credentials: "include",
        });
        const data = await response.json();
        if (data.success) {
          setScore(data.data);
        }
      } catch (error) {
        console.error("Failed to fetch fraud score:", error);
      } finally {
        setLoading(false);
      }
    };
    fetchScore();
  }, [listingId]);

  if (loading) {
    return (
      <Badge variant="outline" className="animate-pulse">
        ...
      </Badge>
    );
  }

  if (!score) {
    return null;
  }

  const getColor = () => {
    if (score.risk_level === "incoherent") return "bg-red-500 hover:bg-red-600";
    if (score.risk_level === "suspect")
      return "bg-yellow-500 hover:bg-yellow-600";
    return "bg-green-500 hover:bg-green-600";
  };

  const getIcon = () => {
    if (score.risk_level === "incoherent")
      return <ShieldAlert className="h-3 w-3" />;
    if (score.risk_level === "suspect")
      return <AlertTriangle className="h-3 w-3" />;
    return <ShieldCheck className="h-3 w-3" />;
  };

  const getLabel = () => {
    if (score.risk_level === "incoherent") return "Dangerous";
    if (score.risk_level === "suspect") return "Suspicious";
    return "Reliable";
  };

  const tooltipContent = (
    <div className="space-y-2 p-1 max-w-xs">
      <div className="flex items-center gap-2">
        <Shield className="h-4 w-4 text-primary" />
        <span className="font-semibold">
          Reliability Score: {(score.score * 100).toFixed(0)}%
        </span>
      </div>
      <div className="text-xs space-y-1">
        <p>
          • Risk level: <strong>{score.risk_level}</strong>
        </p>
        {score.price_deviation !== 0 && (
          <p>
            • Price deviation:{" "}
            <strong
              className={
                score.price_deviation > 0 ? "text-red-500" : "text-green-500"
              }
            >
              {score.price_deviation > 0 ? "+" : ""}
              {score.price_deviation}%
            </strong>{" "}
            vs market
          </p>
        )}
        {score.mismatch_types && score.mismatch_types.length > 0 && (
          <p>
            • Detected anomalies:{" "}
            {Array.isArray(score.mismatch_types)
              ? score.mismatch_types.slice(0, 3).join(", ")
              : score.mismatch_types}
          </p>
        )}
        <p>• Images analyzed: {score.images_analyzed}</p>
      </div>
    </div>
  );

  const sizeClasses = {
    sm: "text-[10px] px-1.5 py-0.5 gap-0.5",
    md: "text-xs px-2 py-1 gap-1",
    lg: "text-sm px-3 py-1.5 gap-1.5",
  };

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Badge
            className={`${getColor()} text-white ${sizeClasses[size]} flex items-center cursor-help`}
          >
            {getIcon()}
            {showLabel && getLabel()}
            <Info className="h-2.5 w-2.5 opacity-70" />
          </Badge>
        </TooltipTrigger>
        <TooltipContent side="top" className="bg-slate-900 text-white">
          {tooltipContent}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
