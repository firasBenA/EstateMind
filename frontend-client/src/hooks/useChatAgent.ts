/**
 * frontend-client/src/hooks/useChatAgent.ts
 * 
 * Custom hook for SSE connection to chat agent.
 * Handles streaming responses, reconnection, error handling.
 */

import { useEffect, useState, useCallback, useRef } from 'react';

interface ChatEventData {
  type: 'token' | 'tool_call' | 'action_required' | 'error' | 'end';
  content?: string;
  tool?: string;
  args?: Record<string, any>;
  result?: Record<string, any>;
  action?: string;
  preview?: Record<string, any>;
  message?: string;
}

interface UseChatAgentOptions {
  onMessage?: (event: ChatEventData) => void;
  sessionId?: string;
  maxRetries?: number;
}

export function useChatAgent(options: UseChatAgentOptions) {
  const {
    onMessage,
    sessionId = '',
    maxRetries = 3,
  } = options;

  const [isConnected, setIsConnected] = useState(true);
  const retryCountRef = useRef(0);
  const eventSourceRef = useRef<EventSource | null>(null);

  const sendMessage = useCallback(async (message: string) => {
    try {
      const baseUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
      const csrfToken = getCsrfToken();

      const response = await fetch(`${baseUrl}/api/chat/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': csrfToken,
        },
        credentials: 'include',
        body: JSON.stringify({
          message,
          session_id: sessionId,
        }),
      });

      if (!response.ok) {
        if (response.status === 429) {
          onMessage?.({
            type: 'error',
            content: 'Rate limit exceeded. Please wait a moment.',
          });
        } else {
          const error = await response.json().catch(() => ({}));
          onMessage?.({
            type: 'error',
            content: error.error || `Error: ${response.statusText}`,
          });
        }
        return;
      }

      // Handle SSE stream
      if (response.body) {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          for (const line of lines) {
            if (line.startsWith('data: ')) {
              try {
                const json = JSON.parse(line.slice(6));
                onMessage?.(json as ChatEventData);
                retryCountRef.current = 0; // Reset retry count on success
              } catch (e) {
                console.error('Failed to parse SSE message:', e);
              }
            }
          }
        }
      }
    } catch (error) {
      console.error('Chat error:', error);
      retryCountRef.current++;

      if (retryCountRef.current < maxRetries) {
        // Retry with exponential backoff
        const delay = Math.min(1000 * Math.pow(2, retryCountRef.current), 5000);
        setTimeout(() => sendMessage(message), delay);
      } else {
        onMessage?.({
          type: 'error',
          content: 'Connection failed. Please try again.',
        });
        setIsConnected(false);
      }
    }
  }, [onMessage, sessionId, maxRetries]);

  return {
    sendMessage,
    isConnected,
  };
}

/**
 * Extract CSRF token from cookies
 */
function getCsrfToken(): string {
  const name = 'csrftoken';
  const match = document.cookie.match(new RegExp(`(^|;\\s*)${name}=([^;]*)`));
  return match ? decodeURIComponent(match[2]) : '';
}
