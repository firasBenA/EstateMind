import { Building2, LayoutDashboard, List, Bot, Wifi, WifiOff } from "lucide-react";
import { useAuth } from "@/contexts/AuthContext";

const NAV_ITEMS = [
  { label: "Dashboard", icon: LayoutDashboard, active: true },
  { label: "Listings", icon: List },
  { label: "Scrapers", icon: Bot },
];

export function AppSidebar() {
  const { user } = useAuth();

  return (
    <aside className="flex h-screen w-60 flex-col border-r border-border bg-sidebar">
      {/* Logo */}
      <div className="flex items-center gap-3 px-4 py-6 border-b border-border">
        <div className="flex h-16 w-16 items-center justify-center overflow-hidden">
          <img 
            src="/logo.png" 
            alt="EstateMind Logo" 
            className="h-full w-full object-contain dark:invert"
            onError={(e) => {
              e.currentTarget.style.display = 'none';
              e.currentTarget.nextElementSibling?.classList.remove('hidden');
            }}
          />
          
        </div>
        <div>
          <h1 className="text-lg font-bold text-foreground tracking-tight">EstateMind</h1>
          <p className="text-xs text-muted-foreground uppercase tracking-widest">Admin</p>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3 py-4 space-y-1">
        {NAV_ITEMS.map((item) => (
          <button
            key={item.label}
            className={`flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors ${
              item.active
                ? "bg-accent text-accent-foreground"
                : "text-sidebar-foreground hover:bg-accent/50 hover:text-foreground"
            }`}
          >
            <item.icon className="h-4 w-4" />
            {item.label}
          </button>
        ))}
      </nav>

      {/* Footer session status */}
      <div className="border-t border-border px-4 py-4">
        <div className="flex items-center gap-2">
          {user ? (
            <>
              <Wifi className="h-3.5 w-3.5 text-success" />
              <span className="text-xs text-muted-foreground">Online</span>
              <span className="ml-auto text-xs font-medium text-foreground">{user.username}</span>
            </>
          ) : (
            <>
              <WifiOff className="h-3.5 w-3.5 text-destructive" />
              <span className="text-xs text-muted-foreground">Offline</span>
            </>
          )}
        </div>
      </div>
    </aside>
  );
}
