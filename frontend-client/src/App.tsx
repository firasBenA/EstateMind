import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes, Navigate } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AuthProvider, useAuth } from "@/lib/auth-context";
import { ThemeProvider } from "@/lib/theme-context";
import { ProtectedRoute } from "@/components/ProtectedRoute";
import { ChatBot } from "@/components/ChatBot"; // ✨ NEW: Agentic chatbot

import LandingPage from "./pages/LandingPage";
import SearchPage from "./pages/SearchPage";
import ListingDetailPage from "./pages/ListingDetailPage";
import LoginPage from "./pages/LoginPage";
import RegisterPage from "./pages/RegisterPage";
import DashboardOverview from "./pages/dashboard/DashboardOverview";
import ListingsManager from "./pages/dashboard/ListingsManager";
import FraudCenter from "./pages/dashboard/FraudCenter";
import AnalyticsPage from "./pages/dashboard/AnalyticsPage";
import PipelinePage from "./pages/dashboard/PipelinePage";
import SettingsPage from "./pages/dashboard/SettingsPage";
import UserDashboard from "./pages/user/UserDashboard";
import UserListings from "./pages/user/UserListings";
import PostListing from "./pages/user/PostListing";
import ProfitabilityPlanner from "./pages/user/ProfitabilityPlanner";
import UserReports from "./pages/user/UserReports";
import UserContracts from "./pages/user/UserContracts";
import UserMessages from "./pages/user/UserMessages";
import UserSettings from "./pages/user/UserSettings";
import NotFound from "./pages/NotFound";
import RecommendationsPage from "@/pages/user/recommendation";

const queryClient = new QueryClient();

function UserRoute({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, user } = useAuth();
  if (!isAuthenticated) return <Navigate to="/login" replace />;
  if (user?.role === "admin" || user?.role === "analyst") return <Navigate to="/dashboard" replace />;
  return <>{children}</>;
}

const App = () => (
  <QueryClientProvider client={queryClient}>
    <ThemeProvider>
      <AuthProvider>
        <TooltipProvider>
          <Toaster />
          <Sonner />
          <ChatBot /> {/* ✨ NEW: Floating chatbot widget */}
          <BrowserRouter>
            <Routes>
              {/* Public */}
              <Route path="/" element={<LandingPage />} />
              <Route path="/search" element={<SearchPage />} />
              <Route path="/listing/:id" element={<ListingDetailPage />} />
              <Route path="/login" element={<LoginPage />} />
              <Route path="/register" element={<RegisterPage />} />
              <Route path="/post-listing" element={<Navigate to="/user/post-listing" replace />} />

              {/* User Dashboard */}
              <Route path="/user" element={<UserRoute><UserDashboard /></UserRoute>} />
              <Route path="/user/listings" element={<UserRoute><UserListings /></UserRoute>} />
              <Route path="/user/post-listing" element={<UserRoute><PostListing /></UserRoute>} />
              <Route path="/user/profitability" element={<UserRoute><ProfitabilityPlanner /></UserRoute>} />
              <Route path="/user/reports" element={<UserRoute><UserReports /></UserRoute>} />
              <Route path="/user/contracts" element={<UserRoute><UserContracts /></UserRoute>} />
              <Route path="/user/recommendations" element={<RecommendationsPage />} />
              <Route path="/user/messages" element={<UserRoute><UserMessages /></UserRoute>} />
              <Route path="/user/settings" element={<UserRoute><UserSettings /></UserRoute>} />

              {/* Admin Dashboard */}
              <Route path="/dashboard" element={<ProtectedRoute><DashboardOverview /></ProtectedRoute>} />
              <Route path="/dashboard/listings" element={<ProtectedRoute><ListingsManager /></ProtectedRoute>} />
              <Route path="/dashboard/fraud" element={<ProtectedRoute><FraudCenter /></ProtectedRoute>} />
              <Route path="/dashboard/analytics" element={<ProtectedRoute><AnalyticsPage /></ProtectedRoute>} />
              <Route path="/dashboard/pipeline" element={<ProtectedRoute><PipelinePage /></ProtectedRoute>} />
              <Route path="/dashboard/settings" element={<ProtectedRoute><SettingsPage /></ProtectedRoute>} />

              <Route path="*" element={<NotFound />} />
            </Routes>
          </BrowserRouter>
        </TooltipProvider>
      </AuthProvider>
    </ThemeProvider>
  </QueryClientProvider>
);

export default App;
