/**
 * frontend-client/src/components/ChatBot.tsx
 *
 * Main chatbot widget with pure CSS styling + Contract Generation.
 * Features:
 * - Floating button to open/close
 * - Message streaming with SSE
 * - Action confirmation modal (listings + contracts)
 * - Contract generation flow (inline in chat)
 * - Typing indicator, error handling, session persistence
 */

import { useState, useRef, useEffect } from 'react';
import { useChatAgent } from '@/hooks/useChatAgent';
import { parseMarkdown, MarkdownNode } from '@/utils/markdown';
import { toast } from 'sonner';
import {
  FileText, Copy, Download, Send, Loader2, AlertCircle,
  MessageCircle, X, CheckCircle2
} from 'lucide-react';
import '../styles/chatbot.css';

// ── Types ────────────────────────────────────────────────────────────────────

interface Message {
  role: 'user' | 'assistant' | 'tool' | 'error' | 'contract';
  content: string;
  timestamp: number;
  toolCall?: {
    tool: string;
    args: Record<string, any>;
    result: Record<string, any>;
  };
  contractData?: {
    contractText: string;
    contractType: string;
    params: Record<string, any>;
    contractId?: number;
  };
}

interface ActionRequiredEvent {
  type: 'action_required';
  action: 'create_listing' | 'generate_contract' | 'save_contract';
  preview: Record<string, any>;
  message: string;
  params?: Record<string, any>;
}

interface ChatEvent {
  type: 'session' | 'token' | 'action_required' | 'tool_call' | 'error' | 'end';
  content?: string;
  session_id?: string;
  tool?: string;
  args?: Record<string, any>;
  result?: Record<string, any>;
  action?: string;
  preview?: Record<string, any>;
  message?: string;
  error?: string;
}

// ── Markdown Renderer ───────────────────────────────────────────────────────

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

// ── Contract Actions Component ─────────────────────────────────────────────


// 🔹 Update ContractActions to accept the full message:
// Update the component signature:
function ContractActions({
  message,  // ← Can be undefined
  onCopy,
  onExport,
  onSend
}: {
  message?: Message;  // ← Make optional with ?
  onCopy: () => void;
  onExport: () => void;
  onSend: () => void;
}) {
  // 🔹 DEFENSIVE: Return early if message or contractData is missing
  if (!message?.contractData) {
    return (
      <div className="chatbot-contract-actions">
        <span className="text-xs text-muted-foreground">Loading...</span>
      </div>
    );
  }

  const { contractData } = message;

  return (
    <div className="chatbot-contract-actions">
      <button
        onClick={onCopy}
        className="chatbot-contract-btn"
        title="Copy to clipboard"
      >
        <Copy className="h-3.5 w-3.5 mr-1" /> Copy
      </button>

      {/* Save button - only show if not already saved */}
      {!contractData.contractId && (
        <button
          onClick={() => {
            // This will be wired up from parent
          }}
          className="chatbot-contract-btn chatbot-contract-btn-primary"
          title="Save contract"
        >
          💾 Save
        </button>
      )}

      {/* Show export/send only if saved */}
      {contractData.contractId && (
        <>
          <button
            onClick={onExport}
            className="chatbot-contract-btn"
            title="Export as PDF"
          >
            <Download className="h-3.5 w-3.5 mr-1" /> PDF
          </button>
          <button
            onClick={onSend}
            className="chatbot-contract-btn chatbot-contract-btn-primary"
            title="Send for signature"
          >
            <Send className="h-3.5 w-3.5 mr-1" /> Send
          </button>
        </>
      )}
    </div>
  );
}

// ── Main ChatBot Component ──────────────────────────────────────────────────

export function ChatBot() {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [actionRequired, setActionRequired] = useState<ActionRequiredEvent | null>(null);

  // Contract generation state
  const [contractDraft, setContractDraft] = useState<Record<string, any> | null>(null);
  const [isGeneratingContract, setIsGeneratingContract] = useState(false);
  const [generatedContractId, setGeneratedContractId] = useState<number | null>(null);

  const messagesEndRef = useRef<HTMLDivElement>(null);

  const ContractActions = ({ message }: { message?: Message }) => {
    if (!message?.contractData) return null;

    const { contractData } = message;

    return (
      <div className="chatbot-contract-actions">
        <button
          onClick={() => handleCopyContract(contractData.contractText)}
          className="chatbot-contract-btn"
          title="Copy"
        >
          <Copy className="h-3.5 w-3.5 mr-1" /> Copy
        </button>

        {/* Save button */}
        {!contractData.contractId && (
          <button
            onClick={async () => {
              const savedId = await handleSaveContract(
                contractData.contractText,
                contractData.params
              );
              if (savedId) {
                // Update the message with the new ID
                setMessages(prev => prev.map(m =>
                  m === message
                    ? { ...m, contractData: { ...m.contractData!, contractId: savedId } }
                    : m
                ));
              }
            }}
            className="chatbot-contract-btn chatbot-contract-btn-primary"
            title="Save"
          >
            💾 Save
          </button>
        )}

        {/* Export/Send buttons (only if saved) */}
        {contractData.contractId && (
          <>
            <button
              onClick={() => handleExportContract(contractData.contractId!)}
              className="chatbot-contract-btn"
              title="PDF"
            >
              <Download className="h-3.5 w-3.5 mr-1" /> PDF
            </button>
            <button
              onClick={() => contractData.params?.buyer_name &&
                handleSendContract(contractData.contractId!, contractData.params.buyer_name)}
              className="chatbot-contract-btn chatbot-contract-btn-primary"
              title="Send"
            >
              <Send className="h-3.5 w-3.5 mr-1" /> Send
            </button>
          </>
        )}
      </div>
    );
  };

  // Session ID lifecycle
  const sessionIdRef = useRef<string>(
    sessionStorage.getItem('chatbot_session_id') || ''
  );

  const { sendMessage, isConnected } = useChatAgent({
    onMessage: (event: ChatEvent) => {
      // Session ID assignment
      if (event.type === 'session') {
        const newId = event.session_id as string;
        if (newId && newId !== sessionIdRef.current) {
          sessionIdRef.current = newId;
          sessionStorage.setItem('chatbot_session_id', newId);
        }
        return;
      }

      // 🔹 Streaming token - handle for both assistant AND contract
      if (event.type === 'token') {
        setMessages(prev => {
          if (prev.length === 0) return prev;
          const last = prev[prev.length - 1];

          // Handle both assistant and contract roles
          if (last.role === 'assistant' || last.role === 'contract') {
            return [
              ...prev.slice(0, -1),
              {
                ...last,
                content: last.content + (event.content || ''),
                contractData: last.contractData ? {
                  ...last.contractData,
                  contractText: (last.contractData.contractText || '') + (event.content || '')
                } : undefined
              },
            ];
          }
          return prev;
        });
        return;
      }

      // Action required (for save confirmation)
      if (event.type === 'action_required') {
        setActionRequired(event as ActionRequiredEvent);
        setIsLoading(false);
        return;
      }

      // Tool call result
      if (event.type === 'tool_call') {
        setMessages(prev => [
          ...prev,
          {
            role: 'tool',
            content: `Tool: ${event.tool}`,
            timestamp: Date.now(),
            toolCall: event,
          },
        ]);
        return;
      }

      // Error
      if (event.type === 'error') {
        setMessages(prev => [
          ...prev,
          { role: 'error', content: event.content || event.error || 'Unknown error', timestamp: Date.now() },
        ]);
        setIsLoading(false);
        return;
      }

      // Stream end
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
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // ── Contract Generation Helpers ───────────────────────────────────────────

  const detectContractIntent = (message: string): boolean => {
    const lower = message.toLowerCase();
    return [
      'generate contract', 'create contract', 'make contract',
      'sales agreement', 'compromis de vente', 'promesse de vente',
      'rental agreement', 'contrat de location', 'acte de vente',
      'generate a contract', 'i need a contract', 'prepare a contract'
    ].some(keyword => lower.includes(keyword));
  };

  // In ChatBot.tsx, add/replace this function:

  // 🔹 FIX: Change signature to accept (text, params) instead of (message)
  const handleSaveContract = async (contractText: string, params: Record<string, any>) => {
    // 🔹 Validate before sending
    if (!contractText?.trim()) {
      toast.error('❌ Contract is empty - please wait for generation to complete');
      console.error('Save failed: contractText is empty');
      return null;
    }

    if (!params.contract_type) {
      toast.error('❌ Missing contract type');
      console.error('Save failed: contract_type is missing');
      return null;
    }

    try {
      console.log('📤 Saving contract:', {
        type: params.contract_type,
        contentLength: contractText.length,
        contentPreview: contractText.slice(0, 100) + '...',
        hasBuyer: !!params.buyer_name,
      });

      const resp = await fetch('/api/contracts/save/', {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
        },
        body: JSON.stringify({
          contract_type: params.contract_type,
          title: `${params.contract_type.replace(/_/g, ' ')} - ${params.listing_title || 'Property'}`,
          params: {
            buyer_name: params.buyer_name || '',
            buyer_cin: params.buyer_cin || '',
            seller_name: params.seller_name || '',
            seller_cin: params.seller_cin || '',
            listing_id: params.listing_id || '',
            listing_title: params.listing_title || '',
            ...params,
          },
          content: contractText.trim(), // ← This should now have content
        }),
      });

      console.log('📥 Response status:', resp.status);

      if (!resp.ok) {
        const error = await resp.json().catch(() => ({}));
        console.error('❌ Save failed:', resp.status, error);
        throw new Error(error.error || `HTTP ${resp.status}`);
      }

      const data = await resp.json();
      console.log('✅ Saved:', data);
      return data.id;

    } catch (error) {
      console.error('💥 Save error:', error);
      toast.error(`Failed to save: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return null;
    }
  };

  const handleContractGeneration = async (params: Record<string, any>) => {
    setIsGeneratingContract(true);
    console.log('[ChatBot] Starting contract generation', params);

    try {
      const resp = await fetch('/api/contracts/generate/', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contract_type: params.contract_type || 'compromis_de_vente',
          params,
        }),
      });

      console.log('[ChatBot] Contract generation response', resp.status, resp.headers.get('content-type'));
      if (!resp.ok) {
        const text = await resp.text().catch(() => '');
        throw new Error(`HTTP ${resp.status}: ${text}`);
      }
      if (!resp.body) throw new Error('No response body');

      // Add placeholder for contract content
      setMessages(prev => [
        ...prev,
        {
          role: 'contract',
          content: '📝 Generating your contract...',
          timestamp: Date.now(),
          contractData: {
            contractText: '',
            contractType: params.contract_type || 'compromis_de_vente',
            params
          }
        },
      ]);

      // Stream the contract
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let contractText = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n\n');
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed.startsWith('data:')) continue;
          try {
            const payload = trimmed.slice(5).trim();
            const evt = JSON.parse(payload);
            console.log('[ChatBot] SSE event', evt);
            if (evt.error) {
              setMessages(prev => [...prev, {
                role: 'error',
                content: `Contract error: ${evt.error}`,
                timestamp: Date.now()
              }]);
              break;
            }
            if (evt.token) {
              contractText += evt.token;
              setMessages(prev => {
                const last = prev[prev.length - 1];
                if (last.role === 'contract' && last.contractData) {
                  return [
                    ...prev.slice(0, -1),
                    {
                      ...last,
                      content: contractText,
                      contractData: { ...last.contractData, contractText },
                    },
                  ];
                }
                return prev;
              });
            }
          } catch (err) {
            console.warn('Ignored malformed SSE line', err, line);
          }
        }
      }

      if (buffer.trim()) {
        const trimmed = buffer.trim();
        if (trimmed.startsWith('data:')) {
          try {
            const payload = trimmed.slice(5).trim();
            const evt = JSON.parse(payload);
            console.log('[ChatBot] SSE leftover event', evt);
            if (evt.token) contractText += evt.token;
          } catch (err) {
            console.warn('Ignored leftover malformed SSE line', err, buffer);
          }
        }
      }

      console.log('[ChatBot] Contract generation finished, text length=', contractText.length);

      // Save contract and get ID
      const saveResp = await fetch('/api/contracts/save/', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contract_type: params.contract_type || 'compromis_de_vente',
          title: `${(params.contract_type || 'compromis_de_vente').replace(/_/g, ' ')} - ${params.listing_title || 'Property'}`,
          params: { buyer_name: params.buyer_name, listing_id: params.listing_id },
          content: contractText,
        }),
      });

      if (saveResp.ok) {
        const data = await saveResp.json();
        setGeneratedContractId(data.id);
        setMessages(prev => {
          const last = prev[prev.length - 1];
          if (last.role === 'contract' && last.contractData) {
            return [
              ...prev.slice(0, -1),
              {
                ...last,
                contractData: { ...last.contractData, contractId: data.id },
              },
            ];
          }
          return prev;
        });
      }

    } catch (error) {
      setMessages(prev => [...prev, {
        role: 'error',
        content: `Failed to generate contract: ${error instanceof Error ? error.message : 'Unknown error'}`,
        timestamp: Date.now()
      }]);
    } finally {
      setIsGeneratingContract(false);
    }
  };

  // ── Message Handlers ─────────────────────────────────────────────────────

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!inputValue.trim() || isLoading) return;

    const userMessage = inputValue.trim();
    setInputValue('');
    setIsLoading(true);

    // Add user message
    setMessages(prev => [
      ...prev,
      { role: 'user', content: userMessage, timestamp: Date.now() },
    ]);

    // Check for contract intent
    if (detectContractIntent(userMessage)) {
      console.log('[ChatBot] Contract intent detected', userMessage);
      // Extract basic params from message
      const extracted: Record<string, any> = {};

      // Contract type
      if (userMessage.toLowerCase().includes('sales agreement') || userMessage.toLowerCase().includes('compromis')) {
        extracted.contract_type = 'compromis_de_vente';
      } else if (userMessage.toLowerCase().includes('promesse')) {
        extracted.contract_type = 'promesse_de_vente';
      } else if (userMessage.toLowerCase().includes('rental') || userMessage.toLowerCase().includes('location')) {
        extracted.contract_type = 'contrat_de_location';
      } else if (userMessage.toLowerCase().includes('acte de vente') || userMessage.toLowerCase().includes('final deed')) {
        extracted.contract_type = 'acte_de_vente';
      }

      // Extract city/location
      const cityMatch = userMessage.match(/(?:in|at|from)\s+([A-Z][a-z]+)/);
      if (cityMatch) extracted.listing_address = cityMatch[1].trim();

      setContractDraft(prev => ({ ...prev, ...extracted }));

      // If we have enough info, ask for confirmation
      if (extracted.contract_type) {
        setActionRequired({
          type: 'generate_contract',
          action: 'generate_contract',
          message: `Generate ${extracted.contract_type.replace(/_/g, ' ')}${extracted.listing_address ? ` for ${extracted.listing_address}` : ''}?`,
          preview: { ...extracted },
          params: extracted,
        });
        setIsLoading(false);
        return;
      }
    }

    // Add assistant placeholder
    setMessages(prev => [
      ...prev,
      { role: 'assistant', content: '', timestamp: Date.now() },
    ]);

    // Send to agent
    await sendMessage(userMessage);
  };

  const handleActionConfirm = async () => {
    if (!actionRequired) return;

    if (actionRequired.action === 'generate_contract' && actionRequired.params) {
      // Merge draft with confirmed params
      const finalParams: Record<string, any> = {
        contract_type: actionRequired.params.contract_type || 'compromis_de_vente',
        seller_name: actionRequired.params.seller_name || '',
        seller_cin: actionRequired.params.seller_cin || '',
        seller_address: actionRequired.params.seller_address || '',
        buyer_name: actionRequired.params.buyer_name || '',
        buyer_cin: actionRequired.params.buyer_cin || '',
        buyer_address: actionRequired.params.buyer_address || '',
        listing_id: actionRequired.params.listing_id || '',
        listing_title: actionRequired.params.listing_title || '',
        listing_address: actionRequired.params.listing_address || '',
        surface: actionRequired.params.surface || 0,
        price: actionRequired.params.price || 0,
        transaction_date: actionRequired.params.transaction_date || new Date().toISOString().split('T')[0],
        transaction_type: actionRequired.params.transaction_type || 'sale',
        ...actionRequired.params,
      };

      setActionRequired(null);
      await handleContractGeneration(finalParams);

    } else if (actionRequired.action === 'save_contract') {
      // Handle contract save confirmation
      setActionRequired(null);
      // TODO: Implement save logic if needed
    } else if (actionRequired.action === 'create_listing') {
      // Existing listing creation logic
      setActionRequired(null);
      setIsLoading(true);
      // TODO: Re-send with confirmation flag
      setIsLoading(false);
    }
  };

  const handleActionCancel = () => {
    setActionRequired(null);
    setMessages(prev => [
      ...prev,
      {
        role: 'assistant',
        content: 'Action cancelled. How else can I help?',
        timestamp: Date.now(),
      },
    ]);
  };

  // Contract action handlers
  const handleCopyContract = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success('✅ Contract copied to clipboard!');
  };

  const handleExportContract = (contractId: number) => {
    window.open(`/api/contracts/${contractId}/pdf/`, '_blank');
  };

  const handleSendContract = (contractId: number, buyerName: string) => {
    fetch(`/api/contracts/${contractId}/send/`, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: buyerName }),
    })
      .then(resp => {
        if (resp.ok) {
          toast.success(`✅ Contract sent to ${buyerName} for signature!`);
        } else {
          toast.error('❌ Failed to send contract');
        }
      })
      .catch(() => toast.error('❌ Network error'));
  };

  // ── Render ───────────────────────────────────────────────────────────────

  return (
    <>
      {/* Floating Button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="chatbot-floating-btn"
        title={isOpen ? 'Close chat' : 'Open chat'}
        aria-label="Toggle chatbot"
      >
        {isOpen ? <X className="h-5 w-5" /> : <MessageCircle className="h-5 w-5" />}
      </button>

      {/* Chat Widget */}
      {isOpen && (
        <div className="chatbot-widget">
          {/* Header */}
          <div className="chatbot-header">
            <div className="flex items-center justify-between w-full">
              <div>
                <h3 className="text-lg font-semibold">EstateMind Assistant</h3>
                <p className="text-xs text-muted-foreground">
                  {isConnected ? '🟢 Online' : '🔴 Offline'}
                </p>
              </div>
              <button
                onClick={() => setIsOpen(false)}
                className="p-1 hover:bg-muted rounded-full transition-colors"
                aria-label="Close chat"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>

          {/* Messages */}
          <div className="chatbot-messages">
            {messages.length === 0 && (
              <div className="chatbot-empty">
                <div className="flex flex-col items-center justify-center h-full text-center">
                  <MessageCircle className="h-12 w-12 text-muted-foreground mb-4" />
                  <p className="font-medium mb-2">👋 Hi! I can help you:</p>
                  <ul className="text-sm text-muted-foreground space-y-1 mb-4">
                    <li>🔍 Search for listings</li>
                    <li>💰 Predict property prices</li>
                    <li>📊 View market analytics</li>
                    <li>📝 Create listings or contracts</li>
                  </ul>
                  <p className="text-xs text-muted-foreground italic">
                    Try: <em>"Generate a sales agreement for my property in Bizerte"</em>
                  </p>
                </div>
              </div>
            )}

            {messages.map((msg, i) => (
              <div key={i} className={`chatbot-message ${msg.role}`}>
                <div className="chatbot-message-content">
                  {/* Typing indicator for last message */}
                  {msg.role === 'contract' && isGeneratingContract && i === messages.length - 1 && !msg.contractData?.contractText ? (
                    <div className="flex items-center gap-2 text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      <span>Generating contract...</span>
                    </div>
                  ) : msg.role === 'contract' && msg.contractData ? (
                    // Contract content with actions
                    <div className="chatbot-contract-message">
                      <div className="chatbot-contract-header">
                        <FileText className="h-4 w-4" />
                        <span className="font-semibold">
                          {msg.contractData.contractType?.replace(/_/g, ' ') || 'Contract'}
                        </span>
                        {msg.contractData.contractId && (
                          <span className="text-xs text-green-600 flex items-center gap-1 ml-auto">
                            <CheckCircle2 className="h-3 w-3" /> Saved
                          </span>
                        )}
                      </div>
                      <div className="chatbot-contract-body">
                        <div className="prose prose-sm max-w-none">
                          <MarkdownContent text={msg.contractData.contractText || msg.content} />
                        </div>
                      </div>

                      {/* 🔹 Inline Contract Actions (fixed - no separate component needed) */}
                      <div className="chatbot-contract-actions">
                        {/* Copy Button */}
                        <button
                          onClick={() => {
                            navigator.clipboard.writeText(msg.contractData?.contractText || '');
                            toast.success('✅ Contract copied to clipboard!');
                          }}
                          className="chatbot-contract-btn"
                          title="Copy to clipboard"
                        >
                          <Copy className="h-3.5 w-3.5 mr-1" /> Copy
                        </button>

                        {/* Save Button - only show if not already saved */}
                        {!msg.contractData.contractId && (
                          <button
                            onClick={async () => {
                              // 🔹 Validate before saving
                              if (!msg.contractData?.contractText?.trim()) {
                                toast.error('❌ Contract is empty');
                                return;
                              }

                              const savedId = await handleSaveContract(
                                msg.contractData.contractText,
                                msg.contractData.params || {}
                              );

                              // 🔹 Update message with new contract ID if save succeeded
                              if (savedId) {
                                setMessages(prev => prev.map(m =>
                                  m === msg
                                    ? {
                                      ...m,
                                      contractData: {
                                        ...m.contractData!,
                                        contractId: savedId
                                      }
                                    }
                                    : m
                                ));
                                toast.success('✅ Contract saved!');
                              }
                            }}
                            className="chatbot-contract-btn chatbot-contract-btn-primary"
                            title="Save contract to database"
                          >
                            💾 Save
                          </button>
                        )}

                        {/* Export/Send Buttons - only show if saved */}
                        {msg.contractData.contractId && (
                          <>
                            <button
                              onClick={() => handleExportContract(msg.contractData.contractId!)}
                              className="chatbot-contract-btn"
                              title="Export as PDF"
                            >
                              <Download className="h-3.5 w-3.5 mr-1" /> PDF
                            </button>
                            <button
                              onClick={() => {
                                const buyerName = msg.contractData.params?.buyer_name;
                                if (buyerName) {
                                  handleSendContract(msg.contractData.contractId!, buyerName);
                                } else {
                                  toast.error('❌ Buyer name not found');
                                }
                              }}
                              className="chatbot-contract-btn chatbot-contract-btn-primary"
                              title="Send for signature"
                            >
                              <Send className="h-3.5 w-3.5 mr-1" /> Send
                            </button>
                          </>
                        )}
                      </div>
                    </div>
                  ) : msg.role === 'assistant' && isLoading && i === messages.length - 1 && !msg.content ? (
                    <span className="chatbot-typing-indicator">
                      <span /><span /><span />
                    </span>
                  ) : msg.role === 'assistant' ? (
                    <MarkdownContent text={msg.content} />
                  ) : msg.role === 'tool' ? (
                    <pre className="chatbot-tool-call">
                      {JSON.stringify(msg.toolCall?.result, null, 2).slice(0, 200)}...
                    </pre>
                  ) : msg.role === 'error' ? (
                    <div className="flex items-start gap-2 text-red-600">
                      <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
                      <p>{msg.content}</p>
                    </div>
                  ) : (
                    <p>{msg.content}</p>
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
                <h4 className="text-lg font-semibold mb-2">
                  {actionRequired.action === 'generate_contract'
                    ? '📝 Confirm Contract Generation'
                    : actionRequired.action === 'save_contract'
                      ? '💾 Save Contract'
                      : '✅ Confirm Action'}
                </h4>
                <p className="text-sm text-muted-foreground mb-4">{actionRequired.message}</p>

                {actionRequired.preview && (
                  <div className="chatbot-modal-preview bg-muted/50 p-3 rounded-lg mb-4 text-xs">
                    <strong>Details:</strong>
                    <pre className="mt-2 whitespace-pre-wrap">{JSON.stringify(actionRequired.preview, null, 2)}</pre>
                  </div>
                )}

                <div className="chatbot-modal-actions flex gap-2 justify-end">
                  <button
                    onClick={handleActionCancel}
                    className="chatbot-btn-secondary px-4 py-2 rounded-lg border hover:bg-muted transition-colors"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={handleActionConfirm}
                    className="chatbot-btn-primary px-4 py-2 rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 transition-colors"
                  >
                    {actionRequired.action === 'generate_contract' ? 'Generate Contract' :
                      actionRequired.action === 'save_contract' ? 'Save' : 'Confirm'}
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
              placeholder="Ask me anything... (e.g., 'Generate sales agreement for my Bizerte land')"
              disabled={isLoading || !isConnected || isGeneratingContract}
              className="chatbot-input"
            />
            <button
              type="submit"
              disabled={isLoading || !inputValue.trim() || !isConnected || isGeneratingContract}
              className="chatbot-send-btn"
            >
              {isLoading || isGeneratingContract ? <Loader2 className="h-4 w-4 animate-spin" /> : '→'}
            </button>
          </form>
        </div>
      )}
    </>
  );
}