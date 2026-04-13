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

export default function UserDashboard() {
  const { user } = useAuth();

  return (
    <UserDashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Welcome back, {user?.name || "User"}!</h1>
          <p className="text-muted-foreground">Here's an overview of your listings and activity.</p>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {stats.map(s => (
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
              {[
                { text: "Someone viewed your listing in La Marsa", time: "2 hours ago" },
                { text: "New like on 'Modern Apartment in Les Berges'", time: "5 hours ago" },
                { text: "Price alert: Your area's avg price increased by 3%", time: "1 day ago" },
                { text: "Investment report ready for download", time: "3 days ago" },
              ].map((item, i) => (
                <div key={i} className="flex items-start justify-between gap-4 py-2 border-b last:border-0">
                  <p className="text-sm">{item.text}</p>
                  <span className="text-xs text-muted-foreground whitespace-nowrap">{item.time}</span>
                </div>
              ))}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-base">Quick Actions</CardTitle></CardHeader>
            <CardContent className="grid grid-cols-2 gap-3">
              {[
                { label: "Post Listing", href: "/user/post-listing", icon: List },
                { label: "Profitability", href: "/user/profitability", icon: TrendingUp },
                { label: "Generate Report", href: "/user/reports", icon: FileText },
                { label: "Messages", href: "/user/messages", icon: MessageCircle },
              ].map(a => (
                <a key={a.label} href={a.href} className="flex items-center gap-3 p-3 rounded-lg border hover:bg-accent transition-colors">
                  <a.icon className="h-5 w-5 text-primary" />
                  <span className="text-sm font-medium">{a.label}</span>
                </a>
              ))}
            </CardContent>
          </Card>
        </div>
      </div>
    </UserDashboardLayout>
  );
}
