import { useAuth } from "@/contexts/AuthContext";
import { LoginCard } from "@/components/LoginCard";
import { AppSidebar } from "@/components/AppSidebar";
import { DashboardHeader } from "@/components/DashboardHeader";
import { MetricsGrid } from "@/components/MetricsGrid";
import { DashboardTabs } from "@/components/DashboardTabs";
import { EDADashboard } from "@/components/EDADashboard";

const Index = () => {
  const { user, loading } = useAuth();

  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      </div>
    );
  }

  if (!user) {
    return <LoginCard />;
  }

  return (
    <div className="flex h-screen w-full overflow-hidden bg-background">
      <AppSidebar />
      <div className="flex flex-1 flex-col h-full overflow-hidden">
        <DashboardHeader />
        <main className="flex-1 overflow-y-auto p-6 space-y-6">
          <MetricsGrid />
          <EDADashboard />
          <DashboardTabs />
        </main>
      </div>
    </div>
  );
};

export default Index;
