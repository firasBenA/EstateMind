/**
 * frontend-client/src/components/ChatBot.tsx
 *
 * Main chatbot widget with modern design.
 * Features:
 * - Floating button to open/close
 * - Message streaming with SSE
 * - Action confirmation modal
 * - Typing indicator
 * - Error handling
 * - Session persistence across messages
 */

import { useState, useRef, useEffect } from 'react';
import { MessageCircle, X, Send, Sparkles, Bot, Check, AlertCircle } from 'lucide-react';
import { useChatAgent } from '@/hooks/useChatAgent';
import { parseMarkdown, MarkdownNode } from '@/utils/markdown';
import { cn } from '@/lib/utils';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { ScrollArea } from './ui/scroll-area';

/**
 * Component to render markdown content with links.
 */
function MarkdownContent({ text }: { text: string }) {
  const nodes = parseMarkdown(text);

  return (
    <div className="text-sm space-y-1">
      {nodes.map((node, idx) => {
        if (node.type === 'text') {
          return <span key={idx}>{node.content}</span>;
        } else if (node.type === 'link') {
          if (!node.isAllowed) {
            return <span key={idx} className="text-muted-foreground line-through">{node.content}</span>;
          }
          return (
            <a
              key={idx}
              href={node.href}
              target="_blank"
              rel="noopener noreferrer"
              className="text-primary hover:underline"
            >
              {node.content}
            </a>
          );
        } else if (node.type === 'bold') {
          return <strong key={idx} className="font-semibold">{node.content}</strong>;
        } else if (node.type === 'italic') {
          return <em key={idx} className="italic">{node.content}</em>;
        }
        return null;
      })}
    </div>
  );
}

interface Message {
  role: 'user' | 'assistant' | 'tool' | 'error';
  content: string;
  timestamp: number;
  toolCall?: {
    tool: string;
    args: Record<string, any>;
    result: Record<string, any>;
  };
}

interface ActionRequiredEvent {
  type: 'action_required';
  action: string;
  preview: Record<string, any>;
  message: string;
}

export function ChatBot() {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [actionRequired, setActionRequired] = useState<ActionRequiredEvent | null>(null);

  const messagesEndRef = useRef<HTMLDivElement>(null);

  /**
   * Session ID lifecycle:
   * 1. On first load → read from sessionStorage (empty string if none)
   * 2. After first backend response → backend sends {"type":"session","session_id":"..."}
   * 3. We store it in the ref AND in sessionStorage immediately
   * 4. Every subsequent sendMessage call reads from the ref → same session
   */
  const sessionIdRef = useRef<string>(
    sessionStorage.getItem('chatbot_session_id') || ''
  );

  const { sendMessage, isConnected } = useChatAgent({
    onMessage: (event) => {
      // ─── Session ID assignment (first chunk from backend) ───────────────
      if (event.type === 'session') {
        const newId = event.session_id as string;
        if (newId && newId !== sessionIdRef.current) {
          sessionIdRef.current = newId;
          sessionStorage.setItem('chatbot_session_id', newId);
        }
        return; // don't add to chat UI
      }

      // ─── Streaming token ─────────────────────────────────────────────────
      if (event.type === 'token') {
        setMessages(prev => {
          if (prev.length === 0) return prev;
          const last = prev[prev.length - 1];
          if (last.role === 'assistant') {
            return [
              ...prev.slice(0, -1),
              { ...last, content: last.content + event.content },
            ];
          }
          return prev;
        });
        return;
      }

      // ─── Action required (e.g. confirm listing creation) ─────────────────
      if (event.type === 'action_required') {
        setActionRequired(event as ActionRequiredEvent);
        setIsLoading(false);
        return;
      }

      // ─── Tool call result ─────────────────────────────────────────────────
      if (event.type === 'tool_call') {
        setMessages(prev => [
          ...prev,
          {
            role:      'tool',
            content:   `Tool: ${event.tool}`,
            timestamp: Date.now(),
            toolCall:  event,
          },
        ]);
        return;
      }

      // ─── Error ────────────────────────────────────────────────────────────
      if (event.type === 'error') {
        setMessages(prev => [
          ...prev,
          { role: 'error', content: event.content, timestamp: Date.now() },
        ]);
        setIsLoading(false);
        return;
      }

      // ─── Stream end ───────────────────────────────────────────────────────
      if (event.type === 'end') {
        setIsLoading(false);
        return;
      }
    },

    get sessionId() {
      return sessionIdRef.current;
    },
  });

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages]);

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!inputValue.trim() || isLoading) return;

    const userMessage = inputValue.trim();
    setInputValue('');
    setIsLoading(true);

    // Add user message bubble
    setMessages(prev => [
      ...prev,
      { role: 'user', content: userMessage, timestamp: Date.now() },
    ]);

    // Add empty assistant placeholder (tokens stream into this)
    setMessages(prev => [
      ...prev,
      { role: 'assistant', content: '', timestamp: Date.now() },
    ]);

    await sendMessage(userMessage);
  };

  const handleActionConfirm = async () => {
    if (!actionRequired) return;
    setActionRequired(null);
    setIsLoading(true);
    // TODO: re-send last user message with action_confirmation=true
    setIsLoading(false);
  };

  const handleActionCancel = () => {
    setActionRequired(null);
    setMessages(prev => [
      ...prev,
      {
        role:      'assistant',
        content:   'Action cancelled. How else can I help?',
        timestamp: Date.now(),
      },
    ]);
  };

  // Format timestamp to readable time
  const formatTime = (timestamp: number) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };

  return (
    <>
      {/* Floating Button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className={cn(
          "fixed bottom-6 right-6 z-50 h-14 w-14 rounded-full shadow-lg transition-all hover:shadow-xl hover:scale-105 flex items-center justify-center",
          isOpen 
            ? "bg-destructive text-destructive-foreground hover:bg-destructive/90" 
            : "bg-primary text-primary-foreground hover:bg-primary/90"
        )}
        title={isOpen ? 'Close chat' : 'Open chat'}
        aria-label="Toggle chatbot"
      >
        {isOpen ? <X className="h-5 w-5" /> : <MessageCircle className="h-5 w-5" />}
      </button>

      {/* Chat Widget */}
      {isOpen && (
        <div className="fixed bottom-24 right-6 z-50 w-96 h-[550px] rounded-2xl shadow-2xl border bg-card flex flex-col overflow-hidden animate-in slide-in-from-bottom-4 duration-200">
          {/* Header */}
          <div className="flex items-center justify-between px-4 py-3 bg-gradient-to-r from-primary to-primary/80 text-primary-foreground">
            <div className="flex items-center gap-2">
              <Bot className="h-5 w-5" />
              <span className="font-semibold">EstateMind Assistant</span>
            </div>
            <div className="flex items-center gap-2">
              <div className={cn(
                "h-2 w-2 rounded-full",
                isConnected ? "bg-green-400 animate-pulse" : "bg-red-400"
              )} />
              <button 
                onClick={() => setIsOpen(false)} 
                className="p-1 rounded hover:bg-primary-foreground/20 transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>

          {/* Messages */}
          <ScrollArea className="flex-1 p-4">
            {messages.length === 0 && (
              <div className="flex flex-col items-center justify-center h-full text-center space-y-4 py-8">
                <div className="h-16 w-16 rounded-full bg-primary/10 flex items-center justify-center">
                  <Sparkles className="h-8 w-8 text-primary/60" />
                </div>
                <div>
                  <p className="font-medium text-sm">👋 Hi! I can help you:</p>
                  <ul className="mt-3 space-y-1.5 text-sm text-muted-foreground">
                    <li className="flex items-center gap-2">
                      <div className="h-1.5 w-1.5 rounded-full bg-primary" />
                      Search for listings
                    </li>
                    <li className="flex items-center gap-2">
                      <div className="h-1.5 w-1.5 rounded-full bg-primary" />
                      Predict property prices
                    </li>
                    <li className="flex items-center gap-2">
                      <div className="h-1.5 w-1.5 rounded-full bg-primary" />
                      View market analytics
                    </li>
                    <li className="flex items-center gap-2">
                      <div className="h-1.5 w-1.5 rounded-full bg-primary" />
                      Create new listings
                    </li>
                  </ul>
                </div>
              </div>
            )}

            {messages.map((msg, i) => (
              <div
                key={i}
                className={cn(
                  "mb-4 flex",
                  msg.role === 'user' ? "justify-end" : "justify-start"
                )}
              >
                <div
                  className={cn(
                    "max-w-[85%] rounded-2xl px-4 py-2.5",
                    msg.role === 'user'
                      ? "bg-primary text-primary-foreground"
                      : msg.role === 'error'
                      ? "bg-destructive/10 text-destructive border border-destructive/20"
                      : "bg-muted"
                  )}
                >
                  {/* Timestamp */}
                  <div className={cn(
                    "text-[10px] mb-1",
                    msg.role === 'user' ? "text-primary-foreground/70" : "text-muted-foreground"
                  )}>
                    {formatTime(msg.timestamp)}
                  </div>

                  {/* Content */}
                  {msg.role === 'assistant' && isLoading && i === messages.length - 1 ? (
                    <div>
                      {msg.content && <MarkdownContent text={msg.content} />}
                      <div className="flex items-center gap-1 mt-2">
                        <div className="h-1.5 w-1.5 rounded-full bg-muted-foreground/50 animate-bounce" style={{ animationDelay: '0ms' }} />
                        <div className="h-1.5 w-1.5 rounded-full bg-muted-foreground/50 animate-bounce" style={{ animationDelay: '150ms' }} />
                        <div className="h-1.5 w-1.5 rounded-full bg-muted-foreground/50 animate-bounce" style={{ animationDelay: '300ms' }} />
                      </div>
                    </div>
                  ) : msg.role === 'assistant' ? (
                    <MarkdownContent text={msg.content} />
                  ) : msg.role === 'error' ? (
                    <div className="flex items-start gap-2">
                      <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
                      <span>{msg.content}</span>
                    </div>
                  ) : (
                    <p className="text-sm whitespace-pre-wrap break-words">{msg.content}</p>
                  )}

                  {/* Tool call result preview */}
                  {msg.toolCall && (
                    <details className="mt-2 text-xs opacity-70">
                      <summary className="cursor-pointer">Tool result</summary>
                      <pre className="mt-1 p-1 rounded bg-black/10 overflow-x-auto">
                        {JSON.stringify(msg.toolCall.result, null, 2).slice(0, 200)}...
                      </pre>
                    </details>
                  )}
                </div>
              </div>
            ))}

            <div ref={messagesEndRef} />
          </ScrollArea>

          {/* Action Confirmation Modal */}
          {actionRequired && (
            <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/50">
              <div className="bg-card rounded-xl shadow-xl w-80 p-5 mx-4">
                <div className="flex items-center gap-2 mb-3">
                  <div className="h-8 w-8 rounded-full bg-primary/10 flex items-center justify-center">
                    <Check className="h-4 w-4 text-primary" />
                  </div>
                  <h4 className="font-semibold">Confirm Action</h4>
                </div>
                <p className="text-sm text-muted-foreground mb-3">{actionRequired.message}</p>
                {actionRequired.preview && (
                  <pre className="text-xs bg-muted p-2 rounded-md mb-4 overflow-x-auto">
                    {JSON.stringify(actionRequired.preview, null, 2).slice(0, 200)}
                    {Object.keys(actionRequired.preview).length > 0 && '...'}
                  </pre>
                )}
                <div className="flex gap-2">
                  <Button onClick={handleActionConfirm} size="sm" className="flex-1">
                    Confirm
                  </Button>
                  <Button onClick={handleActionCancel} variant="outline" size="sm" className="flex-1">
                    Cancel
                  </Button>
                </div>
              </div>
            </div>
          )}

          {/* Input Form */}
          <form onSubmit={handleSendMessage} className="p-4 pt-2 border-t">
            <div className="flex gap-2">
              <Input
                value={inputValue}
                onChange={e => setInputValue(e.target.value)}
                placeholder={isConnected ? "Ask me anything..." : "Connecting..."}
                disabled={isLoading || !isConnected}
                className="flex-1"
              />
              <Button
                type="submit"
                size="icon"
                disabled={isLoading || !inputValue.trim() || !isConnected}
                className="shrink-0"
              >
                {isLoading ? (
                  <div className="h-4 w-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                ) : (
                  <Send className="h-4 w-4" />
                )}
              </Button>
            </div>
            <p className="text-[10px] text-muted-foreground text-center mt-2">
              AI responses may take a few seconds
            </p>
          </form>
        </div>
      )}
    </>
  );
}