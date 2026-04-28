/**
 * frontend-client/src/components/ChatBot.tsx
 *
 * Main chatbot widget with pure CSS styling.
 * Features:
 * - Floating button to open/close
 * - Message streaming with SSE
 * - Action confirmation modal
 * - Typing indicator
 * - Error handling
 * - Session persistence across messages
 */

import { useState, useRef, useEffect } from 'react';
import { useChatAgent } from '@/hooks/useChatAgent';
import { parseMarkdown, MarkdownNode } from '@/utils/markdown';
import '../styles/chatbot.css';

/**
 * Component to render markdown content with links.
 * Only allows internal localhost:8081 links.
 */
function MarkdownContent({ text }: { text: string }) {
  const nodes = parseMarkdown(text);

  return (
    <div className="chatbot-markdown-content">
      {nodes.map((node, idx) => {
        if (node.type === 'text') {
          return <span key={idx}>{node.content}</span>;
        } else if (node.type === 'link') {
          if (!node.isAllowed) {
            return <span key={idx} className="chatbot-blocked-link">{node.content}</span>;
          }
          return (
            <a
              key={idx}
              href={node.href}
              target="_blank"
              rel="noopener noreferrer"
              className="chatbot-link"
            >
              {node.content}
            </a>
          );
        } else if (node.type === 'bold') {
          return <strong key={idx}>{node.content}</strong>;
        } else if (node.type === 'italic') {
          return <em key={idx}>{node.content}</em>;
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
  const [isOpen, setIsOpen]               = useState(false);
  const [messages, setMessages]           = useState<Message[]>([]);
  const [inputValue, setInputValue]       = useState('');
  const [isLoading, setIsLoading]         = useState(false);
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

    // Always read session_id from the ref so it's up-to-date on every send
    get sessionId() {
      return sessionIdRef.current;
    },
  });

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
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

    // Send — useChatAgent reads sessionId via the getter above
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

  return (
    <>
      {/* Floating Button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="chatbot-floating-btn"
        title={isOpen ? 'Close chat' : 'Open chat'}
        aria-label="Toggle chatbot"
      >
        {isOpen ? '✕' : '💬'}
      </button>

      {/* Chat Widget */}
      {isOpen && (
        <div className="chatbot-widget">
          {/* Header */}
          <div className="chatbot-header">
            <h3>EstateMind Assistant</h3>
            <p className="chatbot-subtitle">
              {isConnected ? '🟢 Online' : '🔴 Offline'}
            </p>
          </div>

          {/* Messages */}
          <div className="chatbot-messages">
            {messages.length === 0 && (
              <div className="chatbot-empty">
                <p>👋 Hi! I can help you:</p>
                <ul>
                  <li>Search for listings</li>
                  <li>Predict property prices</li>
                  <li>View market analytics</li>
                  <li>Create new listings</li>
                </ul>
              </div>
            )}

            {messages.map((msg, i) => (
              <div key={i} className={`chatbot-message ${msg.role}`}>
                <div className="chatbot-message-content">
                  {msg.role === 'assistant' && isLoading && i === messages.length - 1 ? (
                    <>
                      {msg.content && <MarkdownContent text={msg.content} />}
                      <span className="chatbot-typing-indicator">
                        <span /><span /><span />
                      </span>
                    </>
                  ) : msg.role === 'assistant' ? (
                    <MarkdownContent text={msg.content} />
                  ) : (
                    <p>{msg.content}</p>
                  )}

                  {msg.toolCall && (
                    <pre className="chatbot-tool-call">
                      {JSON.stringify(msg.toolCall.result, null, 2).slice(0, 200)}...
                    </pre>
                  )}
                </div>
              </div>
            ))}

            <div ref={messagesEndRef} />
          </div>

          {/* Action Confirmation Modal */}
          {actionRequired && (
            <div className="chatbot-modal-overlay">
              <div className="chatbot-modal">
                <h4>Confirm Action</h4>
                <p>{actionRequired.message}</p>
                {actionRequired.preview && (
                  <pre className="chatbot-modal-preview">
                    {JSON.stringify(actionRequired.preview, null, 2)}
                  </pre>
                )}
                <div className="chatbot-modal-actions">
                  <button onClick={handleActionConfirm} className="chatbot-btn-primary">
                    Confirm
                  </button>
                  <button onClick={handleActionCancel} className="chatbot-btn-secondary">
                    Cancel
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* Input */}
          <form onSubmit={handleSendMessage} className="chatbot-input-form">
            <input
              type="text"
              value={inputValue}
              onChange={e => setInputValue(e.target.value)}
              placeholder="Ask me anything..."
              disabled={isLoading || !isConnected}
              className="chatbot-input"
            />
            <button
              type="submit"
              disabled={isLoading || !inputValue.trim() || !isConnected}
              className="chatbot-send-btn"
            >
              {isLoading ? '⏳' : '→'}
            </button>
          </form>
        </div>
      )}
    </>
  );
}