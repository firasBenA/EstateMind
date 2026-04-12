import { Badge } from "@/components/ui/badge";

export function ReliabilityBadge({ level }: { level: "HIGH" | "MEDIUM" | "LOW" }) {
  const styles = {
    HIGH: "bg-success/15 text-success border-success/30",
    MEDIUM: "bg-warning/15 text-warning border-warning/30",
    LOW: "bg-destructive/15 text-destructive border-destructive/30",
  };
  return <Badge variant="outline" className={styles[level]}>{level}</Badge>;
}

export function FraudScoreBadge({ score }: { score: number }) {
  const color = score < 0.3 ? "bg-success/15 text-success" : score < 0.6 ? "bg-warning/15 text-warning" : "bg-destructive/15 text-destructive";
  return <Badge variant="outline" className={color}>{(score * 100).toFixed(0)}%</Badge>;
}

export function TypeBadge({ type }: { type: string }) {
  return <Badge variant="secondary" className="capitalize">{type}</Badge>;
}

export function TransactionBadge({ type }: { type: string }) {
  return (
    <Badge variant="outline" className={type === "sale" ? "border-primary/30 text-primary" : "border-secondary/30 text-secondary"}>
      {type === "sale" ? "For Sale" : "For Rent"}
    </Badge>
  );
}
