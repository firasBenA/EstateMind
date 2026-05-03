import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Send, User, MessageCircle, Loader2 } from "lucide-react";
import { useState, useEffect, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import Pusher from "pusher-js";

interface Conversation {
  id: number;
  listing_id: string;
  listing_title: string;
  listing_image: string | null;
  other_party_name: string;
  other_party_id: number;
  is_buyer: boolean;
  last_message: string;
  last_message_at: string;
  unread_count: number;
}

interface Message {
  id: number;
  message: string;
  sender_id: number;
  receiver_id: number;
  is_read: boolean;
  created_at: string;
  is_mine: boolean;
}

export default function UserMessages() {
  const [searchParams] = useSearchParams();
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeConvId, setActiveConvId] = useState<number | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(true);
  const [sending, setSending] = useState(false);
  const [userId, setUserId] = useState<number | null>(null);
  
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const pusherRef = useRef<any>(null);
  const channelRef = useRef<any>(null);

  // Get current user ID
  useEffect(() => {
    const getUser = async () => {
      try {
        const response = await fetch('/api/session/', {
          credentials: 'include'
        });
        const data = await response.json();
        if (data.is_authenticated) {
          setUserId(data.id);
          localStorage.setItem('userId', data.id.toString());
        }
      } catch (error) {
        console.error('Failed to get user:', error);
      }
    };
    getUser();
  }, []);

  // Load conversations
  useEffect(() => {
    const loadConversations = async () => {
      try {
        const response = await fetch('/api/chat/conversations/', {
          credentials: 'include'
        });
        const data = await response.json();
        setConversations(data.conversations || []);
        
        // Check URL for conversation parameter
        const convId = searchParams.get('conversation');
        if (convId && data.conversations?.some(c => c.id === parseInt(convId))) {
          setActiveConvId(parseInt(convId));
        } else if (data.conversations?.length > 0) {
          setActiveConvId(data.conversations[0].id);
        }
      } catch (error) {
        console.error('Failed to load conversations:', error);
      } finally {
        setLoading(false);
      }
    };
    
    loadConversations();
    
    // Cleanup Pusher on unmount
    return () => {
      if (channelRef.current) {
        channelRef.current.unbind_all();
        channelRef.current = null;
      }
      if (pusherRef.current) {
        pusherRef.current.disconnect();
        pusherRef.current = null;
      }
    };
  }, [searchParams]);

  // Load messages when conversation changes
  useEffect(() => {
    if (!activeConvId) return;
    
    const loadMessages = async () => {
      try {
        const response = await fetch(`/api/chat/messages/${activeConvId}/`, {
          credentials: 'include'
        });
        const data = await response.json();
        setMessages(data.messages || []);
        scrollToBottom();
        
        // Clean up old channel
        if (channelRef.current) {
          channelRef.current.unbind_all();
          channelRef.current = null;
        }
        
        // Setup new Pusher connection
        const pusherKey = import.meta.env.VITE_PUSHER_KEY;
        const pusherCluster = import.meta.env.VITE_PUSHER_CLUSTER;
        
        if (!pusherKey) {
          console.warn('Pusher key not configured');
          return;
        }
        
        // Disconnect old pusher instance if exists
        if (pusherRef.current) {
          pusherRef.current.disconnect();
        }
        
        // Create new pusher instance
        pusherRef.current = new Pusher(pusherKey, {
          cluster: pusherCluster || 'eu',
          authEndpoint: '/api/pusher/auth',
          auth: {
            headers: {
              'X-CSRFToken': getCsrfToken(),
            },
          },
        });
        
        // Subscribe to channel
        const channelName = `chat_${activeConvId}`;
        channelRef.current = pusherRef.current.subscribe(channelName);
        
        channelRef.current.bind('new_message', (data: any) => {
          console.log('New message received:', data);
          
          const isMine = data.sender_id === userId;
          
          setMessages(prev => [...prev, {
            id: data.id || Date.now(),
            message: data.message,
            sender_id: data.sender_id,
            receiver_id: data.receiver_id,
            is_read: data.is_read || false,
            created_at: data.created_at || new Date().toISOString(),
            is_mine: isMine
          }]);
          
          scrollToBottom();
          
          // Update conversation list
          if (!isMine) {
            setConversations(prev => prev.map(conv => 
              conv.id === activeConvId 
                ? { ...conv, last_message: data.message, unread_count: conv.unread_count + 1 }
                : conv
            ));
          }
        });
        
      } catch (error) {
        console.error('Failed to load messages:', error);
      }
    };
    
    loadMessages();
  }, [activeConvId, userId]);

  const scrollToBottom = () => {
    setTimeout(() => {
      messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, 100);
  };

  const handleSend = async () => {
    if (!input.trim() || !activeConvId || sending) return;
    
    setSending(true);
    try {
      const response = await fetch('/api/chat/send/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          conversation_id: activeConvId,
          message: input.trim()
        })
      });
      
      if (response.ok) {
        setInput('');
        scrollToBottom();
      } else {
        const error = await response.json();
        alert(error.error || 'Failed to send message');
      }
    } catch (error) {
      console.error('Error sending message:', error);
      alert('Error sending message');
    } finally {
      setSending(false);
    }
  };

  function getCsrfToken(): string {
    const name = "csrftoken";
    const match = document.cookie.match(new RegExp(`(^|;\\s*)${name}=([^;]*)`));
    return match ? decodeURIComponent(match[2]) : "";
  }

  const activeConversation = conversations.find(c => c.id === activeConvId);

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
      <div className="h-[calc(100vh-8rem)] flex gap-4">
        {/* Conversations list */}
        <div className="w-80 shrink-0 border rounded-lg overflow-hidden flex flex-col bg-card">
          <div className="p-3 border-b bg-primary/5">
            <h3 className="font-semibold text-sm">Messages</h3>
            <p className="text-xs text-muted-foreground">{conversations.length} conversations</p>
          </div>
          <div className="flex-1 overflow-auto">
            {conversations.length === 0 ? (
              <div className="p-8 text-center">
                <MessageCircle className="h-8 w-8 mx-auto text-muted-foreground mb-2" />
                <p className="text-sm text-muted-foreground">No conversations yet</p>
                <p className="text-xs text-muted-foreground">Contact a listing owner to start chatting</p>
              </div>
            ) : (
              conversations.map(c => (
                <button
                  key={c.id}
                  onClick={() => setActiveConvId(c.id)}
                  className={`w-full text-left p-3 border-b hover:bg-accent transition-colors ${activeConvId === c.id ? "bg-accent" : ""}`}
                >
                  <div className="flex items-start gap-3">
                    <div className="h-9 w-9 rounded-full bg-primary/10 flex items-center justify-center shrink-0">
                      <User className="h-4 w-4 text-primary" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex justify-between items-center">
                        <span className="text-sm font-medium truncate">{c.other_party_name}</span>
                        <span className="text-xs text-muted-foreground">
                          {c.last_message_at ? new Date(c.last_message_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : ''}
                        </span>
                      </div>
                      <p className="text-xs text-muted-foreground truncate">{c.last_message || 'Start conversation'}</p>
                      <p className="text-xs text-muted-foreground/60 truncate">Re: {c.listing_title}</p>
                    </div>
                    {c.unread_count > 0 && (
                      <Badge className="bg-primary text-primary-foreground h-5 w-5 flex items-center justify-center rounded-full p-0 text-[10px]">
                        {c.unread_count}
                      </Badge>
                    )}
                  </div>
                </button>
              ))
            )}
          </div>
        </div>

        {/* Chat area */}
        {activeConversation ? (
          <Card className="flex-1 flex flex-col overflow-hidden">
            <div className="p-3 border-b flex items-center gap-3">
              <div className="h-8 w-8 rounded-full bg-primary/10 flex items-center justify-center">
                <User className="h-4 w-4 text-primary" />
              </div>
              <div>
                <p className="text-sm font-medium">{activeConversation.other_party_name}</p>
                <p className="text-xs text-muted-foreground">
                  {activeConversation.is_buyer ? 'Buyer' : 'Responsible'} · 
                  Re: {activeConversation.listing_title}
                </p>
              </div>
            </div>

            <div ref={messagesContainerRef} className="flex-1 overflow-auto p-4 space-y-3">
              {messages.length === 0 ? (
                <div className="text-center py-8">
                  <MessageCircle className="h-8 w-8 mx-auto text-muted-foreground mb-2" />
                  <p className="text-sm text-muted-foreground">No messages yet</p>
                  <p className="text-xs text-muted-foreground">Send a message to start the conversation</p>
                </div>
              ) : (
                messages.map(msg => (
                  <div key={msg.id} className={`flex ${msg.is_mine ? "justify-end" : "justify-start"}`}>
                    <div className={`max-w-[70%] rounded-2xl px-4 py-2 ${msg.is_mine ? "bg-primary text-primary-foreground" : "bg-muted"}`}>
                      <p className="text-sm break-words">{msg.message}</p>
                      <p className={`text-[10px] mt-1 ${msg.is_mine ? "text-primary-foreground/60" : "text-muted-foreground"}`}>
                        {new Date(msg.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                      </p>
                    </div>
                  </div>
                ))
              )}
              <div ref={messagesEndRef} />
            </div>

            <div className="p-3 border-t flex gap-2">
              <Input
                value={input}
                onChange={e => setInput(e.target.value)}
                placeholder="Type a message..."
                onKeyDown={e => e.key === "Enter" && !sending && handleSend()}
                className="flex-1"
                disabled={sending}
              />
              <Button size="icon" onClick={handleSend} disabled={!input.trim() || sending}>
                {sending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
              </Button>
            </div>
          </Card>
        ) : (
          <Card className="flex-1 flex items-center justify-center">
            <div className="text-center p-8">
              <MessageCircle className="h-12 w-12 mx-auto text-muted-foreground mb-3" />
              <p className="text-muted-foreground">Select a conversation to start chatting</p>
              <p className="text-xs text-muted-foreground mt-1">Contact a listing owner from any property page</p>
            </div>
          </Card>
        )}
      </div>
    </UserDashboardLayout>
  );
}