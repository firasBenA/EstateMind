import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useAuth } from "@/lib/auth-context";
import { Eye, Heart, List, TrendingUp, MessageCircle, FileText } from "lucide-react";

const stats = [
  { label: "Active Listings", value: "3", icon: List, change: "+1 this month" },
  { label: "Total Views", value: "1,247", icon: Eye, change: "+18% vs last month" },
  { label: "Total Likes", value: "89", icon: Heart, change: "+12 this week" },
  { label: "Messages", value: "14", icon: MessageCircle, change: "3 unread" },
  { label: "Reports Generated", value: "2", icon: FileText, change: "Last: 3 days ago" },
  { label: "ROI Estimate", value: "+8.2%", icon: TrendingUp, change: "Avg across listings" },
];

import { useEffect, useState } from "react";
import { listingsApi } from "@/lib/api";


export default function UserDashboard() {
  const { user } = useAuth();
  const [stats, setStats] = useState<any>(null);
  const [activities, setActivities] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const [statsData, activityData] = await Promise.all([
          listingsApi.getUserStats(),
          listingsApi.getUserActivity()
        ]);
        setStats(statsData);
        setActivities(activityData.activities);
      } catch (error) {
        console.error("Failed to load dashboard data:", error);
      } finally {
        setLoading(false);
      }
    };
    loadData();
  }, []);

  if (loading) {
    return (
      <UserDashboardLayout>
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
        </div>
      </UserDashboardLayout>
    );
  }

  const statCards = stats ? [
    { label: "Active Listings", value: stats.active_listings, icon: List, change: stats.active_change },
    { label: "Total Views", value: stats.total_views.toLocaleString(), icon: Eye, change: stats.views_change },
    { label: "Total Likes", value: stats.total_likes.toLocaleString(), icon: Heart, change: stats.likes_change },
    { label: "Messages", value: stats.messages, icon: MessageCircle, change: `${stats.unread_messages} unread` },
    { label: "ROI Estimate", value: `+${stats.roi_estimate}%`, icon: TrendingUp, change: "Avg across listings" },
  ] : [];

  return (
    <UserDashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Welcome back, {user?.name || "User"}!</h1>
          <p className="text-muted-foreground">Here's an overview of your listings and activity.</p>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {statCards.map(s => (
            <Card key={s.label}>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">{s.label}</CardTitle>
                <s.icon className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{s.value}</div>
                <p className="text-xs text-muted-foreground mt-1">{s.change}</p>
              </CardContent>
            </Card>
          ))}
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-base">Recent Activity</CardTitle></CardHeader>
            <CardContent className="space-y-3">
              {activities.length > 0 ? activities.map((item, i) => (
                <div key={i} className="flex items-start justify-between gap-4 py-2 border-b last:border-0">
                  <p className="text-sm">{item.text}</p>
                  <span className="text-xs text-muted-foreground whitespace-nowrap">{item.time}</span>
                </div>
              )) : (
                <p className="text-sm text-muted-foreground">No recent activity</p>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-base">Quick Actions</CardTitle></CardHeader>
            <CardContent className="grid grid-cols-2 gap-3">
              <a href="/user/post-listing" className="flex items-center gap-3 p-3 rounded-lg border hover:bg-accent transition-colors">
                <List className="h-5 w-5 text-primary" />
                <span className="text-sm font-medium">Post Listing</span>
              </a>
              <a href="/user/likes" className="flex items-center gap-3 p-3 rounded-lg border hover:bg-accent transition-colors">
                <Heart className="h-5 w-5 text-primary" />
                <span className="text-sm font-medium">Saved Listings</span>
              </a>
              <a href="/user/listings" className="flex items-center gap-3 p-3 rounded-lg border hover:bg-accent transition-colors">
                <Eye className="h-5 w-5 text-primary" />
                <span className="text-sm font-medium">My Listings</span>
              </a>
              <a href="/user/reports" className="flex items-center gap-3 p-3 rounded-lg border hover:bg-accent transition-colors">
                <FileText className="h-5 w-5 text-primary" />
                <span className="text-sm font-medium">Reports</span>
              </a>
            </CardContent>
          </Card>
        </div>
      </div>
    </UserDashboardLayout>
  );
}