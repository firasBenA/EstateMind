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

        {/* Recommendation Tabs */}
        <Tabs defaultValue="for-you" className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="for-you" className="gap-2">
              <Star className="h-4 w-4" />
              For You
            </TabsTrigger>
            <TabsTrigger value="similar" className="gap-2">
              <TrendingUp className="h-4 w-4" />
              Similar to Your Views
            </TabsTrigger>
            <TabsTrigger value="trending" className="gap-2">
              <Flame className="h-4 w-4" />
              Trending
            </TabsTrigger>
          </TabsList>

          <TabsContent value="for-you" className="mt-6">
            <Recommendations key={`for-you-${refreshKey}`}  />
          </TabsContent>

          <TabsContent value="similar" className="mt-6">
            <div className="text-center py-12 text-muted-foreground">
              <TrendingUp className="h-12 w-12 mx-auto mb-3 opacity-50" />
              <p>Similar recommendations will appear based on properties you've viewed</p>
            </div>
          </TabsContent>

          <TabsContent value="trending" className="mt-6">
            <div className="text-center py-12 text-muted-foreground">
              <Flame className="h-12 w-12 mx-auto mb-3 opacity-50" />
              <p>Trending properties in your area coming soon</p>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </UserDashboardLayout>
  );
}