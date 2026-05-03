import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Eye, Heart, MoreVertical, PlusCircle } from "lucide-react";
import { Link } from "react-router-dom";
import { Loader2 } from "lucide-react";
import { useEffect, useState } from "react";
import { listingsApi } from "@/lib/api";

export default function UserListings() {
  const [listings, setListings] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadListings = async () => {
      try {
        const data = await listingsApi.getUserListings();
        setListings(data.listings);
      } catch (error) {
        console.error("Failed to load listings:", error);
      } finally {
        setLoading(false);
      }
    };
    loadListings();
  }, []);

  if (loading) {
    return (
      <UserDashboardLayout>
        <div className="flex items-center justify-center h-64">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      </UserDashboardLayout>
    );
  }

  return (
    <UserDashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">My Listings</h1>
            <p className="text-muted-foreground">Manage and track your property listings</p>
          </div>
          <Button asChild>
            <Link to="/user/post-listing"><PlusCircle className="h-4 w-4 mr-2" /> Post New Listing</Link>
          </Button>
        </div>

        {listings.length === 0 ? (
          <Card>
            <CardContent className="text-center py-12">
              <p className="text-muted-foreground">You haven't posted any listings yet.</p>
              <Button asChild className="mt-4">
                <Link to="/user/post-listing">Post Your First Listing</Link>
              </Button>
            </CardContent>
          </Card>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {listings.map(l => (
              <Card key={l.id} className="overflow-hidden hover:shadow-md transition-shadow">
                <div className="relative h-48">
                  <img src={l.image || "https://placehold.co/400x300"} alt={l.title} className="w-full h-full object-cover" />
                  <Badge className={`absolute top-3 right-3 ${l.status === "active" ? "bg-green-600" : "bg-gray-500"}`}>
                    {l.status === "active" ? "Active" : "Inactive"}
                  </Badge>
                </div>
                <CardContent className="p-4 space-y-3">
                  <h3 className="font-semibold text-sm line-clamp-2">{l.title}</h3>
                  <p className="text-lg font-bold text-primary">{l.price?.toLocaleString()} TND</p>
                  <div className="text-xs text-muted-foreground">{l.city} · {l.rooms} rooms · {l.surface} m²</div>
                  <div className="flex items-center gap-4 pt-2 border-t">
                    <div className="flex items-center gap-1 text-sm text-muted-foreground">
                      <Eye className="h-3.5 w-3.5" /> {l.views || 0}
                    </div>
                    <div className="flex items-center gap-1 text-sm text-muted-foreground">
                      <Heart className="h-3.5 w-3.5" /> {l.likes || 0}
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </div>
    </UserDashboardLayout>
  );
}