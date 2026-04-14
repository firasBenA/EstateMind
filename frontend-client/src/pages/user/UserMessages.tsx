import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Send, User } from "lucide-react";
import { useState } from "react";

const conversations = [
  { id: "conv1", name: "Ahmed Ben Ali", lastMsg: "Is the apartment still available?", time: "2h ago", unread: 2, listing: "Apt Les Berges" },
  { id: "conv2", name: "Sara Khelifi", lastMsg: "Can I schedule a visit?", time: "5h ago", unread: 0, listing: "Villa La Marsa" },
  { id: "conv3", name: "Mohamed Trabelsi", lastMsg: "What's the final price?", time: "1d ago", unread: 0, listing: "Commercial Sfax" },
];

const mockMessages = [
  { id: 1, sender: "them", text: "Hello! I'm interested in your apartment in Les Berges du Lac.", time: "10:30" },
  { id: 2, sender: "me", text: "Hi Ahmed! Yes, it's still available. Would you like to schedule a visit?", time: "10:35" },
  { id: 3, sender: "them", text: "Is the apartment still available?", time: "11:00" },
  { id: 4, sender: "them", text: "I'd like to visit this weekend if possible.", time: "11:01" },
];

export default function UserMessages() {
  const [activeConv, setActiveConv] = useState("conv1");
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState(mockMessages);

  const handleSend = () => {
    if (!input.trim()) return;
    setMessages(m => [...m, { id: m.length + 1, sender: "me", text: input, time: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) }]);
    setInput("");
  };

  return (
    <UserDashboardLayout>
      <div className="h-[calc(100vh-8rem)] flex gap-4">
        {/* Conversations list */}
        <div className="w-80 shrink-0 border rounded-lg overflow-hidden flex flex-col bg-card">
          <div className="p-3 border-b">
            <Input placeholder="Search conversations..." className="h-9" />
          </div>
          <div className="flex-1 overflow-auto">
            {conversations.map(c => (
              <button
                key={c.id}
                onClick={() => setActiveConv(c.id)}
                className={`w-full text-left p-3 border-b hover:bg-accent transition-colors ${activeConv === c.id ? "bg-accent" : ""}`}
              >
                <div className="flex items-start gap-3">
                  <div className="h-9 w-9 rounded-full bg-primary/10 flex items-center justify-center shrink-0">
                    <User className="h-4 w-4 text-primary" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex justify-between items-center">
                      <span className="text-sm font-medium truncate">{c.name}</span>
                      <span className="text-xs text-muted-foreground">{c.time}</span>
                    </div>
                    <p className="text-xs text-muted-foreground truncate">{c.lastMsg}</p>
                    <p className="text-xs text-muted-foreground/60 truncate">Re: {c.listing}</p>
                  </div>
                  {c.unread > 0 && (
                    <Badge className="bg-primary text-primary-foreground h-5 w-5 flex items-center justify-center rounded-full p-0 text-[10px]">
                      {c.unread}
                    </Badge>
                  )}
                </div>
              </button>
            ))}
          </div>
        </div>

        {/* Chat area */}
        <Card className="flex-1 flex flex-col overflow-hidden">
          <div className="p-3 border-b flex items-center gap-3">
            <div className="h-8 w-8 rounded-full bg-primary/10 flex items-center justify-center">
              <User className="h-4 w-4 text-primary" />
            </div>
            <div>
              <p className="text-sm font-medium">{conversations.find(c => c.id === activeConv)?.name}</p>
              <p className="text-xs text-muted-foreground">Re: {conversations.find(c => c.id === activeConv)?.listing}</p>
            </div>
          </div>

          <div className="flex-1 overflow-auto p-4 space-y-3">
            {messages.map(msg => (
              <div key={msg.id} className={`flex ${msg.sender === "me" ? "justify-end" : "justify-start"}`}>
                <div className={`max-w-[70%] rounded-2xl px-4 py-2 ${msg.sender === "me" ? "bg-primary text-primary-foreground" : "bg-muted"}`}>
                  <p className="text-sm">{msg.text}</p>
                  <p className={`text-[10px] mt-1 ${msg.sender === "me" ? "text-primary-foreground/60" : "text-muted-foreground"}`}>{msg.time}</p>
                </div>
              </div>
            ))}
          </div>

          <div className="p-3 border-t flex gap-2">
            <Input
              value={input}
              onChange={e => setInput(e.target.value)}
              placeholder="Type a message..."
              onKeyDown={e => e.key === "Enter" && handleSend()}
              className="flex-1"
            />
            <Button size="icon" onClick={handleSend}>
              <Send className="h-4 w-4" />
            </Button>
          </div>
        </Card>
      </div>
    </UserDashboardLayout>
  );
}
