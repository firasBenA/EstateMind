# 📑 Complete File Index - Agentic Chatbot Implementation

## 🚀 Start Here

**Documentation** (read in this order):
1. `IMPLEMENTATION_SUMMARY.md` ← **Start here** - Overview of what was built
2. `CHATBOT_SETUP_GUIDE.md` - Detailed setup & deployment instructions
3. This file - Navigate the codebase

---

## 📁 Backend Files (Django Agent App)

### Core Agent Logic

```
backend/agent/agent.py (350 lines)
├─ AgentOrchestrator class
│  ├─ __init__() - Initialize LangChain with Together AI
│  ├─ TOOLS_SCHEMA - Tool definitions for LLM
│  ├─ process_message() - Main agent loop (yields SSE chunks)
│  ├─ _extract_search_params() - Parse search from natural language
│  ├─ _extract_predict_params() - Parse price prediction request
│  ├─ _extract_listing_params() - Parse create listing request
│  ├─ _extract_analytics_params() - Parse analytics request
│  ├─ _get_tool_handler() - Route to correct tool
│  ├─ _format_search_response() - Format search results
│  ├─ _format_predict_response() - Format price prediction
│  └─ _format_analytics_response() - Format analytics output
└─ // TODO markers for:
   ├─ Multi-turn conversational context
   ├─ LLM-based intent classification
   ├─ Confidence threshold logic
   └─ More website-specific tools
```

### Views & Streaming

```
backend/agent/views.py (300 lines)
├─ @csrf_exempt chat_endpoint(request)
│  ├─ GET: Return CSRF token
│  └─ POST: Process message, return SSE stream
├─ _get_or_create_session() - Session management
├─ _check_rate_limit() - Rate limiting (10 req/min)
├─ _sse_stream() - Convert generator to SSE format
├─ get_sessions() - List user's chat sessions
├─ get_session_messages() - Retrieve conversation history
└─ delete_session() - Archive old sessions
```

### Tools (Structured Functions)

```
backend/agent/tools/search.py (100 lines)
└─ search_listings()
   ├─ Validate search parameters
   ├─ Build ORM query with filters
   ├─ Apply pagination
   └─ Format results for streaming

backend/agent/tools/predict_price.py (80 lines)
└─ predict_price()
   ├─ Call existing ML predictor
   ├─ Calculate confidence range
   ├─ Generate reasoning text
   └─ Return structured output

backend/agent/tools/crud.py (150 lines)
├─ create_listing() - Create new property (requires confirmation)
├─ update_listing() - Update existing property (requires confirmation)
└─ delete_listing() - Soft delete listing (requires confirmation)
   └─ All return action_required flag for modal confirmation

backend/agent/tools/analytics.py (120 lines)
└─ get_analytics()
   ├─ Aggregate market statistics
   ├─ Calculate fraud rates
   ├─ Get city breakdown
   └─ Return comprehensive metrics
```

### Database Models

```
backend/agent/models.py (80 lines)
├─ ChatSession
│  ├─ user (ForeignKey to User, nullable)
│  ├─ session_id (unique UUID)
│  ├─ title (auto-generated)
│  ├─ created_at, updated_at
│  ├─ is_active (for soft delete)
│  └─ Methods: expire_old_sessions()
│
└─ ChatMessage
   ├─ session (ForeignKey to ChatSession)
   ├─ role (user|assistant|tool|error)
   ├─ content (text)
   ├─ tool_calls (JSONB)
   ├─ tokens_used (for monitoring)
   ├─ created_at
   └─ Factory: from_langchain_message()
```

### Input Validation

```
backend/agent/validators.py (150 lines)
└─ InputValidator class
   ├─ sanitize_string() - XSS prevention
   ├─ validate_price_range() - Constrain prices
   ├─ validate_surface_range() - Constrain surface area
   ├─ validate_rooms() - Constrain room count
   ├─ validate_tool_name() - Whitelist check
   ├─ validate_transaction_type() - sale|rent
   ├─ validate_property_type() - apartment|house|land|commercial
   ├─ validate_listing_creation() - All required fields
   └─ validate_search_params() - Clean search params
```

### Configuration & Admin

```
backend/agent/apps.py (10 lines)
└─ AgentConfig - Django app configuration

backend/agent/admin.py (30 lines)
├─ ChatSessionAdmin
│  └─ List display: session_id, user, title, created_at, is_active
│
└─ ChatMessageAdmin
   └─ List display: session, role, created_at, tokens_used

backend/agent/urls.py (10 lines)
├─ POST /api/chat/ → chat_endpoint (SSE stream)
├─ GET /api/chat/sessions/ → get_sessions
├─ GET /api/chat/sessions/{id}/messages/ → get_session_messages
└─ DELETE /api/chat/sessions/{id}/ → delete_session

backend/agent/migrations/0001_initial.py (60 lines)
└─ Create ChatSession and ChatMessage tables with indexes
```

### Integration Points

```
backend/estate_admin/settings.py
└─ Added "agent" to INSTALLED_APPS (line 29)

backend/estate_admin/urls.py
└─ Added path("api/chat/", include("agent.urls")) (line 19)
```

---

## ⚛️ Frontend Files (React Components)

### Main Chat Widget

```
frontend-client/src/components/ChatBot.tsx (300 lines)
├─ <ChatBot /> component
│  ├─ State: isOpen, messages, inputValue, isLoading
│  ├─ State: actionRequired (for confirmation modal)
│  ├─ useRef: messagesEndRef (auto-scroll)
│  ├─ useRef: sessionIdRef (persist session)
│  │
│  ├─ Floating Button
│  │  └─ 💬 emoji, gradient background, 50x50px
│  │
│  ├─ Chat Widget (modal)
│  │  ├─ Header with gradient background
│  │  ├─ Messages list with auto-scroll
│  │  ├─ Empty state with suggestions
│  │  ├─ Message bubbles (user=green, assistant=white)
│  │  ├─ Typing indicator (3 bouncing dots)
│  │  ├─ Action confirmation modal
│  │  ├─ Input form with send button
│  │  └─ Loading state
│  │
│  └─ Hooks:
│     ├─ useChatAgent() - SSE listener
│     ├─ useEffect() - Auto-scroll
│     └─ useEffect() - Session persistence
│
└─ Handlers:
   ├─ handleSendMessage() - Format & send
   ├─ handleActionConfirm() - Execute confirmed action
   └─ handleActionCancel() - Dismiss modal
```

### SSE Listener Hook

```
frontend-client/src/hooks/useChatAgent.ts (100 lines)
├─ useChatAgent(options) hook
│  ├─ Options: onMessage, sessionId, maxRetries
│  ├─ State: isConnected
│  ├─ Ref: retryCountRef (exponential backoff)
│  │
│  ├─ sendMessage(message) - Async function
│  │  ├─ POST to /api/chat/ with CSRF token
│  │  ├─ Stream SSE events from EventSource
│  │  ├─ Parse JSON from SSE lines
│  │  ├─ Call onMessage callback for each event
│  │  ├─ Retry on failure (exponential backoff)
│  │  └─ Reset retry count on success
│  │
│  └─ Returns: { sendMessage, isConnected }
│
└─ Helper: getCsrfToken() - Extract from cookies
```

### Styling

```
frontend-client/src/styles/chatbot.css (400 lines)
├─ :root variables (colors, spacing)
│
├─ .chatbot-floating-btn
│  ├─ Fixed position (bottom-right)
│  ├─ Gradient background
│  ├─ Hover: scale up
│  └─ Active: scale down
│
├─ .chatbot-widget (modal)
│  ├─ Fixed position (bottom-right, above button)
│  ├─ 380x600px (responsive)
│  ├─ Flexbox column layout
│  ├─ slideUp animation
│  └─ z-index: 999
│
├─ .chatbot-header
│  ├─ Gradient background
│  ├─ Text centered
│  ├─ Status indicator (🟢 Online / 🔴 Offline)
│  └─ Rounded top corners
│
├─ .chatbot-messages (scrollable)
│  ├─ Flex: 1 (takes remaining space)
│  ├─ overflow-y: auto
│  ├─ Light gray background
│  └─ Padding for content
│
├─ .chatbot-message
│  ├─ User messages: green, right-aligned
│  ├─ Assistant messages: white, left-aligned
│  ├─ Error messages: red, left-aligned
│  ├─ Rounded corners with border-radius offset
│  └─ fadeIn animation
│
├─ .chatbot-typing-indicator
│  ├─ 3 bouncing dots
│  └─ bounce animation (0.2s offset)
│
├─ .chatbot-input-form
│  ├─ Flexbox (input + button)
│  ├─ Sticky at bottom
│  └─ Border-top separator
│
├─ .chatbot-modal-overlay
│  ├─ Fixed full-screen
│  ├─ Semi-transparent dark background
│  └─ Centered modal inside
│
└─ @media (max-width: 480px)
   ├─ Responsive design for mobile
   ├─ Widget takes 90vw
   ├─ Height 70vh
   └─ Adjusted positioning
```

### Integration

```
frontend-client/src/App.tsx
├─ Import ChatBot from @/components/ChatBot
└─ Add <ChatBot /> component before <BrowserRouter>
```

---

## ⚙️ Configuration Files

### Environment Variables

```
.env.example (root directory)
├─ TOGETHER_AI_API_KEY=your_key_here (free, required)
├─ DJANGO_SECRET_KEY, DEBUG, ALLOWED_HOSTS
├─ Database config (PG_NAME, PG_USER, PG_PASSWORD, PG_HOST, PG_PORT)
├─ CORS_ALLOWED_ORIGINS
├─ Agent settings (RATE_LIMIT, CLEANUP_DAYS)
└─ LLM settings (TEMPERATURE, MAX_TOKENS, MODEL)
```

### Dependencies

```
backend/requirements.txt
├─ Django==4.2.0
├─ langchain==0.1.7
├─ langchain-community==0.0.13
├─ langchain-together==0.0.1
├─ together==0.2.11 (Together AI SDK)
├─ psycopg2-binary (PostgreSQL)
├─ scikit-learn, xgboost, lightgbm (ML)
├─ sentence-transformers (NLP)
├─ pydantic (validation)
├─ playwright (optional browser fallback)
└─ Other utilities (logging, testing, formatting)
```

### Database Migration

```
backend/agent/migrations/0001_initial.py
├─ Create agent_chatsession table
│  ├─ Fields: id, user_id, session_id, title, created_at, updated_at, is_active
│  └─ Indexes: session_id, (user_id, is_active)
│
└─ Create agent_chatmessage table
   ├─ Fields: id, session_id, role, content, tool_calls, tokens_used, created_at
   └─ Index: (session_id, created_at)
```

---

## 📚 Documentation Files

### Setup & Deployment

```
CHATBOT_SETUP_GUIDE.md (600+ lines)
├─ Quick Start (1 minute)
├─ Backend Setup (step-by-step)
├─ Frontend Setup (step-by-step)
├─ Free LLM Setup (Together AI)
├─ Testing (3 verification tests)
├─ Deployment (Railway + Vercel)
├─ Troubleshooting (common issues)
├─ Architecture Overview
└─ Customization (TODO markers)
```

### Implementation Overview

```
IMPLEMENTATION_SUMMARY.md (this file reference)
├─ What was built (feature list)
├─ File structure and line counts
├─ Quick start commands
├─ Architecture diagram
├─ 4 Tools explained
├─ Features (implemented + TODO)
├─ Performance metrics
├─ Security features
├─ Example queries
├─ Deployment overview
├─ Troubleshooting quick reference
└─ Next steps
```

### This File

```
FILE_INDEX.md (you are here)
├─ Complete navigation of all files
├─ Purpose of each file
├─ Key functions/components
├─ Line counts and complexity
└─ How files connect to each other
```

---

## 🔗 File Dependencies & Data Flow

### Backend Data Flow

```
User Message (HTTP POST)
    ↓
chat_endpoint view (views.py)
    ├─ Parse JSON
    ├─ Create/get ChatSession
    ├─ Rate limit check
    ├─ Sanitize input (validators.py)
    ↓
AgentOrchestrator (agent.py)
    ├─ Initialize LangChain + Together AI
    ├─ Detect intent (heuristic)
    ├─ Route to tool
    ↓
Tool Handler (tools/*.py)
    ├─ Validate parameters
    ├─ Execute (ORM/ML/aggregation)
    └─ Return result
    ↓
Format Response (agent.py)
    ├─ _format_*_response()
    ├─ Yield tokens incrementally
    └─ Yield tool call result
    ↓
Save to DB (models.py)
    ├─ ChatMessage.objects.create()
    └─ Store role, content, tool_calls
    ↓
SSE Stream (views.py)
    ├─ _sse_stream()
    └─ Yield JSON events to client
    ↓
React Client (SSE receiver)
```

### Frontend Data Flow

```
User clicks send button
    ↓
ChatBot.tsx handleSendMessage()
    ├─ Add message to UI
    ├─ Disable input
    ├─ Show loading spinner
    ↓
useChatAgent.ts sendMessage()
    ├─ POST to /api/chat/
    ├─ Include CSRF token
    ├─ Include session_id
    ↓
SSE EventSource listener
    ├─ Parse "data: {...}" lines
    ├─ Call onMessage callback
    ↓
ChatBot.tsx onMessage handler
    ├─ If type="token": append to message content
    ├─ If type="action_required": show modal
    ├─ If type="tool_call": display result
    ├─ If type="error": show error message
    ├─ If type="end": stop loading
    ↓
Update UI
    ├─ Messages re-render
    ├─ Scroll to bottom
    └─ Remove loading indicator
    ↓
User sees streaming response ✨
```

---

## 📊 File Statistics

### Lines of Code by Component

```
Component              | Lines | Type           | Complexity
-----------------------|-------|----------------|------------
Agent Orchestrator     | 350   | Core Logic     | High (LangChain)
Views & Streaming      | 300   | Web Layer      | Medium (SSE)
Search Tool            | 100   | ORM Query      | Low
Predict Price Tool     | 80    | ML Integration | Low
CRUD Tools             | 150   | Business Logic | Medium
Analytics Tool         | 120   | Aggregation    | Low
Input Validation       | 150   | Security       | Medium
Database Models        | 80    | ORM            | Low
Admin Config           | 30    | Django         | Low
URLs & Apps            | 30    | Config         | Low
React ChatBot          | 300   | UI Component   | Medium
SSE Hook               | 100   | React Hook     | Low
CSS Styling            | 400   | Pure CSS       | Low
TOTAL                  | 2,300 | Production     | Mixed
```

### File Organization

```
Backend Complexity Pyramid:
     ↑
    [ agent.py         ] - Orchestration (most complex)
    [ tools/           ] - Tool implementations
    [ views.py         ] - SSE streaming
    [ validators.py    ] - Input validation
    [ models.py        ] - Database models
    [ admin.py/urls.py ] - Configuration (least complex)
    ↓

Frontend Complexity Pyramid:
     ↑
    [ ChatBot.tsx      ] - Main component (medium complex)
    [ useChatAgent.ts  ] - Hook logic (low complex)
    [ chatbot.css      ] - Styling (low complex)
    ↓
```

---

## ✅ Quality Checklist

- ✅ All 4 tools implemented & tested
- ✅ Error handling with graceful fallbacks
- ✅ Input validation & sanitization
- ✅ Rate limiting per session
- ✅ Database migrations included
- ✅ React components responsive
- ✅ Pure CSS (no external dependencies)
- ✅ SSE streaming working
- ✅ Action confirmation modal
- ✅ PostgreSQL integration
- ✅ Free LLM setup (Together AI)
- ✅ Deployment guides (Railway + Vercel)
- ✅ Comprehensive documentation
- ✅ TODO markers for extensions

---

## 🚀 Next Actions

1. **Read**: Start with `IMPLEMENTATION_SUMMARY.md`
2. **Setup**: Follow `CHATBOT_SETUP_GUIDE.md`
3. **Test**: Run locally (5 minutes)
4. **Customize**: Implement TODO markers
5. **Deploy**: Push to Railway + Vercel

---

**Last Updated:** April 25, 2026
**Status:** ✅ Production Ready
**Total Implementation Time:** Complete
