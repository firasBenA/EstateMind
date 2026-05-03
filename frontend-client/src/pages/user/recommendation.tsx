// frontend-client/src/pages/user/RecommendationsPage.tsx
import { useState } from "react";
import Recommendations from "@/components/recommendation";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Sparkles, Star, TrendingUp, RefreshCw, Flame } from "lucide-react";
import { UserDashboardLayout } from "@/components/UserDashboardLayout";

export default function RecommendationsPage() {
  const [refreshKey, setRefreshKey] = useState(0);

  const handleRefresh = () => {
    setRefreshKey(prev => prev + 1);
  };

  return (
    <UserDashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-2">
              <Sparkles className="h-6 w-6 text-primary" />
              Personalized Recommendations
            </h1>
            <p className="text-muted-foreground mt-1">
              AI-powered property suggestions based on your browsing behavior
            </p>
          </div>
          <Button variant="outline" onClick={handleRefresh} className="gap-2">
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
        </div>
        <Recommendations key={`for-you-${refreshKey}`}  />
      </div>
    </UserDashboardLayout>
  );
}