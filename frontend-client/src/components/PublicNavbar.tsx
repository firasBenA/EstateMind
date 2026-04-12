import { Link, useLocation } from "react-router-dom";
import { useAuth } from "@/lib/auth-context";
import { useTheme } from "@/lib/theme-context";
import { Button } from "@/components/ui/button";
import { Sun, Moon, Menu, X } from "lucide-react";
import { useState } from "react";
import logoImg from "@/assets/logo.png";

export function PublicNavbar() {
  const { isAuthenticated, user } = useAuth();
  const { theme, toggle } = useTheme();
  const location = useLocation();
  const [mobileOpen, setMobileOpen] = useState(false);

  const isUserRole = user?.role === "particular" || user?.role === "agency";
  const dashboardLink = isUserRole ? "/user" : "/dashboard";
  const dashboardLabel = isUserRole ? "My Dashboard" : "Admin Dashboard";

  return (
    <header className="sticky top-0 z-50 border-b bg-background/95 backdrop-blur">
      <div className="container mx-auto flex h-16 items-center justify-between px-4">
        <Link to="/" className="flex items-center gap-2">
          <img src={logoImg} alt="EstateMind" className="h-10 w-auto" />
        </Link>

        <nav className="hidden md:flex items-center gap-6">
          <Link to="/search" className={`text-sm font-medium transition-colors hover:text-primary ${location.pathname === "/search" ? "text-primary" : "text-muted-foreground"}`}>
            Search
          </Link>
          <Link to="/post-listing" className={`text-sm font-medium transition-colors hover:text-primary ${location.pathname === "/post-listing" ? "text-primary" : "text-muted-foreground"}`}>
            Post a Listing
          </Link>
          <button onClick={toggle} className="p-2 rounded-md hover:bg-muted transition-colors">
            {theme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
          </button>
          {isAuthenticated ? (
            <Button asChild size="sm"><Link to={dashboardLink}>{dashboardLabel}</Link></Button>
          ) : (
            <div className="flex gap-2">
              <Button asChild variant="outline" size="sm"><Link to="/login">Login</Link></Button>
              <Button asChild size="sm"><Link to="/register">Sign Up</Link></Button>
            </div>
          )}
        </nav>

        <button className="md:hidden p-2" onClick={() => setMobileOpen(!mobileOpen)}>
          {mobileOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
        </button>
      </div>

      {mobileOpen && (
        <div className="md:hidden border-t bg-background p-4 space-y-3">
          <Link to="/search" className="block text-sm font-medium" onClick={() => setMobileOpen(false)}>Search</Link>
          <Link to="/post-listing" className="block text-sm font-medium" onClick={() => setMobileOpen(false)}>Post a Listing</Link>
          <div className="flex items-center justify-between">
            <span className="text-sm">Theme</span>
            <button onClick={toggle} className="p-2 rounded-md hover:bg-muted">
              {theme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
            </button>
          </div>
          {isAuthenticated ? (
            <Button asChild size="sm" className="w-full"><Link to={dashboardLink}>{dashboardLabel}</Link></Button>
          ) : (
            <div className="space-y-2">
              <Button asChild variant="outline" size="sm" className="w-full"><Link to="/login">Login</Link></Button>
              <Button asChild size="sm" className="w-full"><Link to="/register">Sign Up</Link></Button>
            </div>
          )}
        </div>
      )}
    </header>
  );
}
