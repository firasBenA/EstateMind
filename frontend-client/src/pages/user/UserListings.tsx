import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Eye, Heart, MoreVertical, PlusCircle } from "lucide-react";
import { Link } from "react-router-dom";

const userListings = [
  { id: "u1", title: "Modern Apartment in Les Berges du Lac", city: "Tunis", price: 320000, type: "apartment", rooms: 3, surface: 120, views: 487, likes: 34, status: "active", image: "https://images.unsplash.com/photo-1545324418-cc1a3fa10c00?w=400" },
  { id: "u2", title: "Villa with Garden in La Marsa", city: "La Marsa", price: 850000, type: "house", rooms: 5, surface: 280, views: 612, likes: 41, status: "active", image: "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=400" },
  { id: "u3", title: "Commercial Space Downtown", city: "Sfax", price: 180000, type: "commercial", rooms: 2, surface: 90, views: 148, likes: 14, status: "rented", image: "https://images.unsplash.com/photo-1497366216548-37526070297c?w=400" },
];

export default function UserListings() {
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

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {userListings.map(l => (
            <Card key={l.id} className="overflow-hidden hover:shadow-md transition-shadow">
              <div className="relative h-48">
                <img src={l.image} alt={l.title} className="w-full h-full object-cover" />
                <Badge className={`absolute top-3 right-3 ${l.status === "active" ? "bg-success" : "bg-muted"}`}>
                  {l.status === "active" ? "Active" : "Rented"}
                </Badge>
              </div>
              <CardContent className="p-4 space-y-3">
                <h3 className="font-semibold text-sm line-clamp-2">{l.title}</h3>
                <p className="text-lg font-bold text-primary">{l.price.toLocaleString()} TND</p>
                <div className="text-xs text-muted-foreground">{l.city} · {l.rooms} rooms · {l.surface} m²</div>
                <div className="flex items-center gap-4 pt-2 border-t">
                  <div className="flex items-center gap-1 text-sm text-muted-foreground">
                    <Eye className="h-3.5 w-3.5" /> {l.views}
                  </div>
                  <div className="flex items-center gap-1 text-sm text-muted-foreground">
                    <Heart className="h-3.5 w-3.5" /> {l.likes}
                  </div>
                  <Button variant="ghost" size="sm" className="ml-auto h-8 w-8 p-0">
                    <MoreVertical className="h-4 w-4" />
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </UserDashboardLayout>
  );
}
