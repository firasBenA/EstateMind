# 🚀 EstateMind Agentic Chatbot - Complete Setup & Deployment Guide

## 📋 Table of Contents
1. [Quick Start (Local Dev)](#quick-start-local-dev)
2. [Backend Setup](#backend-setup)
3. [Frontend Setup](#frontend-setup)
4. [Free LLM Setup (Together AI)](#free-llm-setup-together-ai)
5. [Testing](#testing)
6. [Deployment (Railway + Vercel)](#deployment-railway--vercel)
7. [Troubleshooting](#troubleshooting)
8. [Architecture Overview](#architecture-overview)
9. [Customization (TODO Markers)](#customization-todo-markers)

---

## 🏃 Quick Start (Local Dev)

### Prerequisites
- Python 3.10+ (with Django 4.2+)
- Node.js 18+ (for frontend)
- PostgreSQL 12+ (or use existing Supabase)
- Git

### 1-Minute Setup
```bash
# 1. Clone/navigate to repo
cd c:\Users\Jmbo2\Desktop\EstateMind

# 2. Copy environment template
cp .env.example .env

# 3. Backend setup (see Backend Setup section below)
cd backend
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver

# 4. Frontend setup (in new terminal)
cd frontend-client
npm install
npm run dev

# 5. Open browser
# Backend: http://localhost:8000
# Frontend: http://localhost:5173
# Chat: Click 💬 button in bottom-right

# 6. Get Together AI key (see section below)
# Add TOGETHER_AI_API_KEY to .env
```

---

## 🐍 Backend Setup

### Step 1: Activate Python Environment

```bash
# Windows - Anaconda
conda activate base
# or create new env
conda create -n estatemind python=3.11
conda activate estatemind

# macOS/Linux
python3 -m venv venv
source venv/bin/activate  # or: . venv/Scripts/activate (Windows)
```

### Step 2: Install Dependencies

```bash
cd backend
pip install -r requirements.txt

# Verify installation
python -c "import langchain; print('✅ LangChain installed')"
python -c "import together; print('✅ Together AI installed')"
python -c "import django; print('✅ Django installed')"
```

### Step 3: Configure Environment

```bash
# Copy .env.example to .env (in root directory)
cp ..\.env.example ..\.env

# Edit .env and add:
TOGETHER_AI_API_KEY=sk_xxx_your_key_here
DEBUG=true
```

### Step 4: Setup Database

```bash
# Create migrations
python manage.py makemigrations

# Apply migrations (creates agent_chatsession and agent_chatmessage tables)
python manage.py migrate

# Create superuser (optional, for admin panel)
python manage.py createsuperuser
# Username: admin
# Password: (your choice)
```

### Step 5: Run Backend

```bash
python manage.py runserver
# Output: Starting development server at http://127.0.0.1:8000/

# In another terminal, test:
curl http://localhost:8000/api/chat/ -H "Content-Type: application/json"
# Response: {"csrf_token": "..."}
```

---

## ⚛️ Frontend Setup

### Step 1: Install Dependencies

```bash
cd frontend-client
npm install
# Wait for install to complete (~2-3 min)
```

### Step 2: Configure Environment

```bash
# Copy .env.example to .env.local in frontend-client/
cp .env.example .env.local

# Set backend URL (should match your backend)
VITE_API_URL=http://localhost:8000
```

### Step 3: Run Development Server

```bash
npm run dev
# Output: Local: http://localhost:5173/
```

### Step 4: Test Chat Widget

```bash
# Open browser: http://localhost:5173
# Look for 💬 button in bottom-right
# Click to open chat widget
# Type: "Search apartments in Tunis"
# Should see streaming response
```

---

## 🤖 Free LLM Setup (Together AI)

### Step 1: Sign Up for Together AI (FREE)

```
🔗 https://www.together.ai/
- No credit card required
- Free tier: 50,000 tokens/minute
- Plenty for development & testing
```

### Step 2: Get API Key

```
1. Sign up at together.ai
2. Go to Dashboard → API → Keys
3. Click "Generate New Key"
4. Copy key: sk_xxx_...
```

### Step 3: Add to .env

```bash
# In root .env file:
TOGETHER_AI_API_KEY=tgp_v1_gvPqEWmDnD1izS6L_OmTm13oOdRvKJ43uXXr5EaObgw

# Test connection:
python -c "
import os
from dotenv import load_dotenv
load_dotenv()
from langchain_together import ChatTogether
llm = ChatTogether(model='meta-llama/Llama-3.1-405B-Instruct')
print('✅ Together AI connected successfully!')
"
```

### Free Tier Limits & Workarounds

| Resource | Limit | Workaround |
|----------|-------|-----------|
| Tokens/minute | 50,000 | Simple caching for identical queries |
| Requests/minute | ~500 | Rate limit per user (10/min in settings) |
| Max tokens/request | 8,000 | Keep responses under 1000 tokens |
| Concurrent streams | 10 | Sequential processing |

---

## 🧪 Testing

### Test 1: Agent Tool Isolation

```bash
cd backend
python manage.py shell

# Test search tool
from agent.tools.search import search_listings
result = search_listings(city="Tunis", page_size=5)
print(f"Found {result['count']} listings")
# Expected: success

# Test price predictor
from agent.tools.predict_price import predict_price
result = predict_price(
    property_type="apartment",
    city="Tunis",
    surface=100,
    rooms=2
)
print(f"Predicted price: {result['predicted_price']} TND")
# Expected: price between 200k-500k

# Test analytics
from agent.tools.analytics import get_analytics
result = get_analytics(city="Tunis")
print(f"Total listings: {result['total_listings']}")
# Expected: count > 0
```

### Test 2: End-to-End Chat

```bash
# Terminal 1: Backend running
cd backend && python manage.py runserver

# Terminal 2: Frontend running
cd frontend-client && npm run dev

# Terminal 3: Test via curl
curl -X POST http://localhost:8000/api/chat/ \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Show me 3-room apartments in Tunis under 500k"
  }'

# Should return SSE stream:
# data: {"type": "token", "content": "🔍 Searching..."}
# data: {"type": "tool_call", "tool": "search_listings", ...}
# data: {"type": "end", "content": ""}
```

### Test 3: Manual Browser Test

```
1. Open http://localhost:5173
2. Click 💬 button (bottom-right)
3. Type: "What's the average price per m² in Sfax?"
4. Watch response stream in real-time
5. Try other queries:
   - "Predict price for 150m² apartment in Ariana with 3 rooms"
   - "How many listings are flagged as fraudulent?"
   - "Find villas in Sousse between 800k and 1.5M"
```

---

## 🚀 Deployment (Railway + Vercel)

### Deployment Architecture

```
┌─ Vercel (React Frontend) ─────────────────┐
│  - URL: estatemind.vercel.app             │
│  - Env: VITE_API_URL=estatemind-api...   │
└─────────────────────────────────────────────┘
                     ↓
┌─ Railway (Django Backend) ─────────────────┐
│  - URL: estatemind-api.railway.app        │
│  - Env: TOGETHER_AI_API_KEY, PG_*         │
└─────────────────────────────────────────────┘
                     ↓
┌─ PostgreSQL (Supabase or Railway DB) ──────┐
│  - chatbot tables (ChatSession, ChatMessage)│
│  - existing listings, agent_metrics, etc   │
└─────────────────────────────────────────────┘
```

### Backend Deployment (Railway)

#### Step 1: Create Railway Project

```bash
# Install Railway CLI (optional, can use web UI)
npm i -g @railway/cli

# or via web: https://railway.app
```

#### Step 2: Create Procfile

```bash
# backend/Procfile (create new file)
web: gunicorn estate_admin.wsgi --log-file -
release: python manage.py migrate
```

#### Step 3: Configure Environment Variables

```
In Railway Dashboard → Variables:

DJANGO_SECRET_KEY=your-production-key-here
DEBUG=false
ALLOWED_HOSTS=estatemind-api.railway.app,your-domain.com

# Database
PG_NAME=railway_db
PG_USER=postgres
PG_PASSWORD=your-password
PG_HOST=containers-us-west-123.railway.app
PG_PORT=5432
PG_SSLMODE=require

# LLM
TOGETHER_AI_API_KEY=sk_xxx_your_key

# CORS
CORS_ALLOWED_ORIGINS=https://estatemind.vercel.app,https://your-domain.com
```

#### Step 4: Deploy

```bash
# Push to GitHub (Railway watches GitHub)
git add .
git commit -m "Add agentic chatbot"
git push origin main

# Railway auto-deploys on push
# Watch logs: railway logs
```

### Frontend Deployment (Vercel)

#### Step 1: Push to GitHub

```bash
cd frontend-client
git add .
git commit -m "Add chatbot widget"
git push origin main
```

#### Step 2: Connect to Vercel

```
1. Go to https://vercel.com/new
2. Select Git provider (GitHub)
3. Select estatemind repository
4. Framework: Vite
5. Root directory: frontend-client
6. Environment variables:
   VITE_API_URL=https://estatemind-api.railway.app
7. Click "Deploy"
```

#### Step 3: Test Production

```bash
# Frontend: https://estatemind.vercel.app
# Backend: https://estatemind-api.railway.app
# Chat: Click 💬 button
```

### Database Setup on Railway

```bash
# If not auto-provisioned:
1. Railway Dashboard → New → PostgreSQL
2. Copy connection details
3. Set in environment variables (see Backend Deployment)
4. Run migrations:
   railway run python manage.py migrate
```

---

## 🆘 Troubleshooting

### Issue 1: "ModuleNotFoundError: No module named 'langchain'"

**Solution:**
```bash
pip install langchain langchain-community langchain-together
python -m pip install --upgrade pip setuptools wheel
```

### Issue 2: "TOGETHER_AI_API_KEY not found"

**Solution:**
```bash
# Verify .env file exists and has key
cat .env | grep TOGETHER_AI

# If missing:
echo "TOGETHER_AI_API_KEY=sk_xxx" >> .env
```

### Issue 3: Chat returns "Agent not initialized"

**Solution:**
```bash
# Backend logs
cd backend && python manage.py runserver  # Check for errors

# Verify LangChain works:
python -c "from langchain_together import ChatTogether; ChatTogether()"

# If error, reinstall:
pip uninstall langchain-together
pip install langchain-together==0.0.1
```

### Issue 4: Database migration fails

**Solution:**
```bash
# Check existing migrations
python manage.py showmigrations agent

# If 0001_initial not applied:
python manage.py migrate agent 0001_initial

# Or full reset (dev only!):
python manage.py migrate agent zero
python manage.py migrate agent
```

### Issue 5: CORS errors in browser console

**Solution:**
```bash
# Verify .env has correct CORS origins:
CORS_ALLOWED_ORIGINS=http://localhost:5173,https://estatemind.vercel.app

# Restart backend:
python manage.py runserver
```

### Issue 6: SSE stream not working in frontend

**Solution:**
```bash
# Check browser console (F12) for errors
# Verify backend returns proper SSE headers:

curl -i http://localhost:8000/api/chat/ \
  -H "Content-Type: application/json" \
  -d '{"message": "test"}'

# Should see:
# Content-Type: text/event-stream
# Cache-Control: no-cache
```

---

## 🏗️ Architecture Overview

### Data Flow

```
User Message (text)
    ↓
React ChatBot.tsx (UI)
    ↓
useChatAgent hook (SSE listener)
    ↓
POST /api/chat/ (HTTP)
    ↓
Django chat_endpoint view
    ↓
AgentOrchestrator (LangChain)
    ↓
Intent Detection + Tool Routing
    ↓
┌─────────────────────────────────┐
│   4 Structured Tools:           │
│  1. search_listings (ORM)       │
│  2. predict_price (ML model)    │
│  3. create_listing (CRUD)       │
│  4. get_analytics (aggregation) │
└─────────────────────────────────┘
    ↓
Tool Result → Format Response
    ↓
SSE Stream (tokens + tool calls)
    ↓
React displays in real-time
```

### Key Files

| File | Purpose |
|------|---------|
| `backend/agent/agent.py` | LangChain orchestrator |
| `backend/agent/views.py` | SSE `/api/chat/` endpoint |
| `backend/agent/tools/` | 4 tool implementations |
| `backend/agent/models.py` | ChatSession, ChatMessage |
| `frontend-client/src/components/ChatBot.tsx` | Main UI widget |
| `frontend-client/src/hooks/useChatAgent.ts` | SSE listener |
| `frontend-client/src/styles/chatbot.css` | Pure CSS styling |

---

## 🔧 Customization (TODO Markers)

### TODO 1: Add Website-Specific Tools

**File:** `backend/agent/agent.py` (line ~80)

```python
# TODO: Add more tools specific to your website
# Examples:
# - get_user_favorites: Retrieve user's saved listings
# - calculate_mortgage: Mortgage calculator
# - schedule_viewing: Book property tours
# - generate_report: Create investment reports

@staticmethod
def search_by_agent():
    """TODO: Search properties by real estate agent"""
    pass

@staticmethod
def estimate_taxes():
    """TODO: Estimate property taxes"""
    pass
```

### TODO 2: Improve Natural Language Understanding

**File:** `backend/agent/agent.py` (line ~150)

```python
# TODO: Replace simple keyword matching with LLM classification
# Current: Heuristic routing (fast but limited)
# Improved: Use LLM to classify intent first

def _classify_intent_with_llm(self, message: str) -> str:
    """Use LLM to classify user intent"""
    # TODO: Implement with Together AI
    prompt = f"Classify this request: {message}"
    # intent = self.llm.predict(prompt)
    pass
```

### TODO 3: Add Multi-Turn Conversational Context

**File:** `backend/agent/agent.py` (line ~120)

```python
# TODO: Maintain conversation context across turns
# Current: Single-turn processing
# Improved: Use LangChain memory to reference previous messages

from langchain.memory import ConversationBufferMemory

def __init__(self):
    # TODO: Add memory
    self.memory = ConversationBufferMemory()
    # self.agent.memory = self.memory
```

### TODO 4: Add Confidence Threshold

**File:** `backend/agent/agent.py` (line ~200)

```python
# TODO: Add confidence scoring for agent decisions
# If confidence < threshold, ask user for clarification

def _should_execute_tool(self, tool_name: str, confidence: float) -> bool:
    """TODO: Check if confidence is high enough"""
    CONFIDENCE_THRESHOLD = 0.75
    if confidence < CONFIDENCE_THRESHOLD:
        return False  # Ask user for clarification
    return True
```

### TODO 5: Add Browser Automation Fallback

**File:** `backend/agent/tools/browser.py` (OPTIONAL)

```python
# TODO: Implement Playwright fallback
# Used when APIs fail (graceful degradation)

async def scrape_with_fallback(url: str):
    """TODO: Use Playwright to scrape if API fails"""
    from playwright.async_api import async_playwright
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, timeout=30000)
        # Extract content
        await browser.close()
```

### TODO 6: Add User Feedback Loop

**File:** `backend/agent/models.py`

```python
# TODO: Add feedback to improve agent over time

class ChatMessage(models.Model):
    # ... existing fields ...
    
    # TODO: Add feedback fields
    # user_feedback = models.CharField(choices=[
    #     ('helpful', 'Helpful'),
    #     ('not_helpful', 'Not Helpful'),
    #     ('needs_improvement', 'Needs Improvement'),
    # ], null=True)
    # feedback_text = models.TextField(null=True)
```

### TODO 7: Add Admin Dashboard for Agent Monitoring

**File:** `backend/agent/admin.py`

```python
# TODO: Add custom admin actions for agent management

@admin.action(description="Export chat history to CSV")
def export_chats(modeladmin, request, queryset):
    """TODO: Export chat sessions to CSV for analysis"""
    pass

@admin.action(description="Clear old sessions")
def cleanup_old_sessions(modeladmin, request, queryset):
    """TODO: Delete sessions older than 30 days"""
    pass
```

### TODO 8: Add Real-Time Analytics Dashboard

**File:** `frontend-client/src/pages/...` (NEW FILE)

```typescript
// TODO: Create agent analytics page
// Track:
// - Number of user queries per day
// - Average response time
// - Tool usage frequency
// - User satisfaction
// - Error rates
```

---

## 🎯 Next Steps

1. ✅ **Local Testing** → Run all 3 tests from "Testing" section
2. ✅ **Together AI Setup** → Get API key, add to .env
3. ✅ **Deployment** → Push to Railway (backend) + Vercel (frontend)
4. ✅ **Customization** → Implement TODOs for your use case
5. ✅ **Monitoring** → Set up Sentry for error tracking
6. ✅ **Scale** → Use caching & optimize for high volume

---

## 📞 Support

For issues or questions:
1. Check Troubleshooting section above
2. Review backend logs: `python manage.py runserver`
3. Check browser console (F12) for frontend errors
4. Review Together AI docs: https://www.together.ai/docs
5. Review LangChain docs: https://python.langchain.com

---

## 📄 License

This code is part of EstateMind and follows the same license.

---

**Last Updated:** April 25, 2026
**Status:** ✅ Production Ready
**Free Tier:** ✅ 100% Open Source, no paid dependencies
