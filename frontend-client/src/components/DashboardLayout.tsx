import { Link, useLocation } from "react-router-dom";
import { useAuth } from "@/lib/auth-context";
import { useTheme } from "@/lib/theme-context";
import {
  LayoutDashboard, List, Shield, BarChart3, Database, Settings, LogOut,
  Sun, Moon, ChevronLeft, ChevronRight, Home,
} from "lucide-react";
import { useState, ReactNode } from "react";
import logoImg from "@/assets/logo.png";

const navItems = [
  { label: "Overview", path: "/dashboard", icon: LayoutDashboard },
  { label: "Listings", path: "/dashboard/listings", icon: List },
  { label: "Fraud Detection", path: "/dashboard/fraud", icon: Shield },
  { label: "Analytics", path: "/dashboard/analytics", icon: BarChart3 },
  { label: "Pipeline", path: "/dashboard/pipeline", icon: Database },
  { label: "Settings", path: "/dashboard/settings", icon: Settings },
];

export function DashboardLayout({ children }: { children: ReactNode }) {
  const [collapsed, setCollapsed] = useState(false);
  const { user, logout } = useAuth();
  const { theme, toggle } = useTheme();
  const location = useLocation();

  return (
    <div className="min-h-screen flex">
      <aside className={`${collapsed ? "w-16" : "w-64"} transition-all duration-300 border-r bg-sidebar flex flex-col shrink-0`}>
        <div className="h-16 flex items-center justify-between px-3 border-b">
          {!collapsed && (
            <Link to="/" className="flex items-center">
              <img src={logoImg} alt="EstateMind" className="h-8 w-auto" />
            </Link>
          )}
          <button onClick={() => setCollapsed(!collapsed)} className="p-1.5 rounded-md hover:bg-sidebar-accent transition-colors">
            {collapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
          </button>
        </div>

        <nav className="flex-1 py-4 space-y-1 px-2">
          {navItems.map(item => {
            const active = location.pathname === item.path;
            return (
              <Link
                key={item.path}
                to={item.path}
                className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  active ? "bg-primary text-primary-foreground" : "text-sidebar-foreground hover:bg-sidebar-accent"
                }`}
                title={collapsed ? item.label : undefined}
              >
                <item.icon className="h-4 w-4 shrink-0" />
                {!collapsed && <span>{item.label}</span>}
              </Link>
            );
          })}
        </nav>

        <div className="p-3 border-t space-y-2">
          <Link to="/" className="flex items-center gap-3 px-3 py-2 rounded-lg text-sm text-sidebar-foreground hover:bg-sidebar-accent transition-colors">
            <Home className="h-4 w-4 shrink-0" />
            {!collapsed && <span>Public Site</span>}
          </Link>
          <button onClick={toggle} className="flex items-center gap-3 px-3 py-2 rounded-lg text-sm text-sidebar-foreground hover:bg-sidebar-accent transition-colors w-full">
            {theme === "dark" ? <Sun className="h-4 w-4 shrink-0" /> : <Moon className="h-4 w-4 shrink-0" />}
            {!collapsed && <span>{theme === "dark" ? "Light Mode" : "Dark Mode"}</span>}
          </button>
          {!collapsed && user && (
            <div className="px-3 py-2 text-xs text-muted-foreground truncate">{user.email}</div>
          )}
          <button onClick={logout} className="flex items-center gap-3 px-3 py-2 rounded-lg text-sm text-destructive hover:bg-destructive/10 transition-colors w-full">
            <LogOut className="h-4 w-4 shrink-0" />
            {!collapsed && <span>Logout</span>}
          </button>
        </div>
      </aside>

      <div className="flex-1 flex flex-col min-w-0">
        <header className="h-16 border-b flex items-center px-6 bg-background shrink-0">
          <h2 className="text-lg font-semibold truncate">
            {navItems.find(n => n.path === location.pathname)?.label || "Dashboard"}
          </h2>
        </header>
        <main className="flex-1 overflow-auto p-6">{children}</main>
      </div>
    </div>
  );
}
