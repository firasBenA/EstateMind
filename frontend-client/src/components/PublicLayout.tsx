import { ReactNode } from "react";
import { PublicNavbar } from "./PublicNavbar";
import { ChatbotButton } from "./ChatbotButton";
import logoImg from "@/assets/logo.png";

export function PublicLayout({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen flex flex-col">
      <PublicNavbar />
      <main className="flex-1">{children}</main>
      <footer className="border-t py-12 bg-muted/30">
        <div className="container mx-auto px-4">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
            <div className="space-y-3">
              <img src={logoImg} alt="EstateMind" className="h-10 w-auto" />
              <p className="text-sm text-muted-foreground">Your trusted real estate analytics platform for the Tunisian market.</p>
            </div>
            <div>
              <h4 className="font-semibold mb-3 text-sm">For Users</h4>
              <ul className="space-y-2 text-sm text-muted-foreground">
                <li>Search Properties</li>
                <li>Post a Listing</li>
                <li>Investment Reports</li>
              </ul>
            </div>
            <div>
              <h4 className="font-semibold mb-3 text-sm">Resources</h4>
              <ul className="space-y-2 text-sm text-muted-foreground">
                <li>Market Analytics</li>
                
                <li>Price Trends</li>
              </ul>
            </div>
            <div>
              <h4 className="font-semibold mb-3 text-sm">Legal</h4>
              <ul className="space-y-2 text-sm text-muted-foreground">
                <li>Privacy Policy</li>
                <li>Terms of Service</li>
                <li>Contact Us</li>
              </ul>
            </div>
          </div>
          <div className="border-t pt-6 text-center text-sm text-muted-foreground">
            © 2026 EstateMind. All rights reserved.
          </div>
        </div>
      </footer>
      
    </div>
  );
}
