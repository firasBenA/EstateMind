import { MessageCircle, X } from "lucide-react";
import { useState } from "react";

export function ChatbotButton() {
  const [open, setOpen] = useState(false);

  return (
    <>
      {open && (
        <div className="fixed bottom-24 right-6 z-50 w-80 h-96 rounded-2xl shadow-2xl border bg-card flex flex-col overflow-hidden animate-in slide-in-from-bottom-4">
          <div className="flex items-center justify-between px-4 py-3 bg-primary text-primary-foreground">
            <span className="font-semibold text-sm">EstateMind Assistant</span>
            <button onClick={() => setOpen(false)} className="p-1 hover:bg-primary-foreground/20 rounded">
              <X className="h-4 w-4" />
            </button>
          </div>
          <div className="flex-1 flex items-center justify-center p-6 text-center">
            <div className="space-y-3">
              <MessageCircle className="h-12 w-12 mx-auto text-muted-foreground/40" />
              <p className="text-sm text-muted-foreground">AI Chatbot coming soon!</p>
              <p className="text-xs text-muted-foreground/60">Ask questions about properties, pricing, and investment opportunities.</p>
            </div>
          </div>
        </div>
      )}
      <button
        onClick={() => setOpen(!open)}
        className="fixed bottom-6 right-6 z-50 h-14 w-14 rounded-full bg-primary text-primary-foreground shadow-lg hover:shadow-xl transition-all hover:scale-105 flex items-center justify-center"
      >
        {open ? <X className="h-6 w-6" /> : <MessageCircle className="h-6 w-6" />}
      </button>
    </>
  );
}
