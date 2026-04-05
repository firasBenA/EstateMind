
# EstateMind

## Overview
EstateMind is a comprehensive real estate data scraping and analysis platform. It aggregates property listings from multiple Tunisian real estate websites, cleans and normalizes the data, and provides an intelligent dashboard for market analysis.

## Architecture

### 1. Scraping Layer (`scrapers/`)
- **Modular Scrapers**: Individual scrapers for each source (Aqari, Century21, Mubawab, etc.).
- **Base Scraper**: `core/base_scraper.py` defines the standard interface and error handling.
- **Intelligent Agent**: `core/agent.py` manages execution, handling:
  - **Strategies**: Aggressive vs. Balanced vs. Conservative (based on error rates).
  - **Cooldowns**: Automatic backoff on repeated failures.
  - **Metrics**: Tracks success/failure rates per source.

### 2. Core Pipeline (`core/`)
- **Pipeline**: `core/pipeline.py` orchestrates the data flow.
- **Geolocation**: `core/geolocation.py` adds lat/lon and district data.
- **POI Extraction**: `core/poi_extractor.py` identifies Points of Interest.
- **Storage**: Saves raw HTML/JSON to `database/raw/` and processed data to PostgreSQL.

### 3. ETL & Data Quality (`scripts/`)
- **Intelligent ETL**: `scripts/etl_intelligent.py` runs after scraping.
  - **Cleaning**: Regex-based cleaning for prices, surfaces, and rooms.
  - **Standardization**: Normalizes locations and property types.
  - **Inference**: Infers missing data (e.g., property type from description).
  - **Quality Scoring**: Assigns a quality score to each listing.

### 4. Database
- **PostgreSQL**: Primary storage for structured listing data.
- **Schema**: `listings` table with JSONB support for flexible attributes (`features`, `images`, `poi`).

### 5. Dashboard (`estatemind-dashboard/`)
- **Frontend**: React + Vite + Tailwind CSS.
- **Features**:
  - Price distribution charts.
  - Interactive map.
  - Market metrics (Average price/m²).

## Directory Structure

```
EstateMind/
├── core/                   # Core logic (Agent, Pipeline, Models)
├── scrapers/               # Individual site scrapers
├── scripts/                # ETL and utility scripts
├── database/               # Database connection and storage logic
├── estatemind-dashboard/   # React frontend dashboard
├── config/                 # Configuration settings
├── logs/                   # Application logs
└── scheduler.py            # Main entry point for the scraping job
```

## Setup & Usage

### Prerequisites
- Python 3.12+
- PostgreSQL
- Node.js (for dashboard)

### Running the Scraper
The main entry point is `scheduler.py`, which initializes the Intelligent Agent.

```bash
python scheduler.py
```

### Running the ETL Manually
To clean existing data without rescraping:

```bash
python scripts/etl_intelligent.py
```

### Starting the Dashboard
Navigate to the dashboard directory:

```bash
cd estatemind-dashboard
npm install
npm run dev
```
---------------------------------------------------------------
data/
├── main.py                        # scheduler
├── scrapers/                      # existing
├── ai_agent/                      # existing  
├── database/
│   ├── vector_db.py               # Pinecone (semantic search)
│   └── timeseries_db.py           # NEW: price history storage
│
├── preprocessing/                 # data cleaning
│   ├── pipeline.py                # runs after each scrape
│   ├── nlp/
│   │   └── extractor.py          # extract fields from description
│   └── steps/
│       ├── deduplicator.py        # cross-source dedup only
│       ├── change_detector.py     # detect price/status changes (Obj 2)
│       ├── null_handler.py        # fill nulls via NLP
│       ├── normalizer.py          # standardize formats
│       ├── outlier_detector.py    # flag statistical outliers (feeds Obj 3)
│       └── scorer.py             # reliability score
│
├── modeling/                      # ML models
│   ├── anomaly/
│   │   └── detector.py           # Obj 3: unsupervised anomaly detection
│   ├── forecasting/
│   │   └── price_forecast.py     # Obj 5: time series forecasting
│   ├── multimodal/
│   │   └── consistency.py        # Obj 4: image/text/price consistency
│   └── recommendations/
│       └── recommender.py        # Obj 8
│
├── rag/                           # LangChain lives here
│   ├── legal_pipeline.py         # Obj 6-7: legal doc RAG
│   └── market_search.py          # semantic property search
│
└── exports/                       # for dashboard and reports
    └── report_generator.py       # Obj 9


    ------------------------------------
    EstateMind — TDSP Progress Recap

What is TDSP
Microsoft's Team Data Science Process. Five phases:
1. Business Understanding    ✓ DONE
2. Data Acquisition          ✓ DONE  
3. Modeling                  ← YOU ARE HERE
4. Deployment                → next
5. Customer Acceptance       → future

Phase 1 — Business Understanding ✓
Nine objectives defined and mapped to technical solutions. Architecture designed. Storage strategy decided (Pinecone + SQLite).

Phase 2 — Data Acquisition & Understanding ✓
✓ 10 scrapers live (8 working, 2 partial — tecnocasa/tunisieannonce)
✓ Railway deployment — runs every 24h without laptop
✓ Pinecone vector DB — 969 vectors and growing
✓ Preprocessing pipeline — all 9 steps working:
    normalize → null_handler → dedup → outlier_flag → score → change_detect → upsert → export
✓ SQLite time series DB — price_history table recording changes
✓ Django + React dashboard — live, reads from Pinecone
✓ Reliability scoring — 0-100 per listing
✓ CSV exports — clean snapshot + model-ready snapshot daily
✓ Backfill utility — eliminates UNKNOWN scores on existing vectors (see [`data/tools/README_BACKFILL.md`](data/tools/README_BACKFILL.md))
Current data quality snapshot:
Total vectors:     969 (growing daily)
Model-ready:       842 (87%)
HIGH quality:      134 (13.8%)
GOOD quality:       29 (3.0%)
LOW quality:       679 (70.0%)  ← surface/rooms still mostly null
DROP:              127 (13.1%)
Cross-source dups:   6 (0.6%)
Outliers:           62 (6.4%)
LOW is high because surface/rooms are missing from most listings — the NLP extractor will fix this once working. This will improve weekly as more data accumulates.

Phase 3 — Modeling (current phase)
Here is every objective mapped to its technical approach, ordered by dependency and feasibility:

Obj 3 — Anomaly Detection ← START HERE
What: flag statistically abnormal listings at regional level (not just rule-based outliers — learned patterns).
Why first: needs least data, teaches you the modeling infrastructure, feeds directly into the reliability score and dashboard.
Stack:
python# No LLM needed — pure sklearn
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

features = ["price", "surface", "rooms", "price_per_m2", 
            "image_count", "description_length"]
# Train per region + property_type group
# Output: anomaly_score per listing → store in Pinecone metadata
```

**File:** `modeling/anomaly/detector.py`

**Input:** `data/exports/model_ready_*.csv`
**Output:** `anomaly_score` and `anomaly_label` added to Pinecone metadata

**Data needed:** 200+ listings per region — you have enough for Tunis now.

---

#### Obj 2 — Change Detection ← ALREADY BUILT, needs activation

**What:** built in `change_detector.py`, writing to SQLite. Just needs to run for 2-3 weeks to accumulate meaningful history.

**Nothing to build** — just let the pipeline run daily. Come back to this for Obj 5.

---

#### Obj 1 — ETL Pipeline ← DONE

Already built and running. The preprocessing pipeline IS the ETL pipeline. Obj 1 is complete.

---

#### Obj 8 — Recommendation System ← needs frontend first

**What:** personalized property recommendations based on search history and user behavior.

**Stack:**
```
User behavior data → collaborative filtering OR content-based
Content-based:  user query vector → Pinecone similarity search (already works)
Collaborative:  needs user accounts + search logs → build after frontend
LLM role here: parse natural language user query into structured filters before Pinecone search.
python# Example flow
user_query = "appartement 3 chambres lac 2 budget 500k"
# → LLM extracts: {rooms: 3, city: "Lac 2", price_max: 500000, type: "Apartment"}
# → Pinecone query with metadata filters
# → ranked results
Blocked on: user accounts, search history logging. Build the frontend user system first.

Obj 5 — Time Series Price Forecasting ← needs 3-6 months data
What: multivariate forecasting of property prices. 1, 3, 5-year forecasts by region.
Stack:
python# Option A — classical (works with less data)
from statsmodels.tsa.statespace.sarimax import SARIMAX
# or
from prophet import Prophet  # Facebook Prophet

# Option B — deep learning (needs 1+ year of data)
# LSTM or Transformer (PyTorch)

# Exogenous variables (macroeconomic indicators):
# - Tunisia inflation rate
# - USD/TND exchange rate  
# - Construction permits issued
# - Population migration data
# → scrape from BCT (Banque Centrale de Tunisie) or INS
Data needed: minimum 90 days of daily price snapshots per region. You have ~5 days now. Come back in 3 months.
File: modeling/forecasting/price_forecast.py

Obj 4 — Multimodal Consistency Classifier ← needs image embeddings
What: detect semantic mismatches — photo shows a villa but description says studio, or price is 50k but listing claims luxury.
Stack:
python# Step 1: embed listing images using CLIP
from transformers import CLIPModel, CLIPProcessor
image_vector = clip.encode_image(image_url)  # 512-dim

# Step 2: store in Pinecone namespace "images"
# Step 3: compare image_vector vs text_vector → cosine distance
# Step 4: train classifier on (image_vec, text_vec, price) → mismatch_score

# LLM role: generate explanation of WHY it's mismatched
Blocked on: need to download and embed listing images. Currently only storing URLs. Build an image embedding pipeline first.
File: modeling/multimodal/consistency.py

Obj 6 & 7 — Legal RAG Pipeline ← LangChain goes here
What:

Obj 6: retrieve relevant legal clauses from Tunisian real estate law documents
Obj 7: LLM generates a contract → Legal module checks each clause against law → flags issues

This is the primary use case for LangChain and LLMs.
python# Stack
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Pinecone  # namespace: "legal_docs"
from langchain.chains import RetrievalQA
from langchain_community.llms import Ollama  # or OpenRouter

# Pipeline
# 1. Chunk legal documents (already have in PostgreSQL — migrate to Pinecone)
# 2. Embed chunks → Pinecone namespace "legal_docs"
# 3. User asks: "what are mandatory clauses for a sale contract in Tunisia?"
# 4. RAG retrieves relevant law chunks
# 5. LLM generates answer grounded in retrieved law

# Obj 7 adds:
# 6. LLM generates full contract from user inputs
# 7. Legal checker: for each clause, RAG-check against law
# 8. Output: contract + flagged issues + missing mandatory clauses
```

**LLM options (free/cheap):**
```
Mistral 7B via Ollama    → runs locally, free, good French
mistralai/mistral-7b via OpenRouter → ~$0.06/1M tokens
claude-haiku via Anthropic API → fast, cheap
```

**Blocked on:** migrating legal documents from PostgreSQL to Pinecone (new namespace).

---

#### Obj 9 — Analytics Dashboard ← built incrementally

**What:** multi-audience interactive dashboard with real-time monitoring, anomaly tracking, investment scoring, exportable reports.

**Current state:** basic dashboard exists (MetricsGrid, EDADashboard, DashboardTabs). Needs:
```
Add to React:
  - Anomaly alerts panel         ← after Obj 3
  - Price trend charts           ← after Obj 5 has data
  - Investment scoring map       ← after Obj 3 + scoring
  - Report export (PDF)          ← after modeling
  - Legal contract generator UI  ← after Obj 6/7
```

---

### Recommended build order
```
NOW (you have enough data):
  Week 1-2:  Obj 3 — Anomaly detection model
  Week 2-3:  Obj 4 setup — image embedding pipeline
  Week 3-4:  Obj 6/7 — Legal RAG (migrate PostgreSQL docs → Pinecone)

WAIT FOR DATA (3 months):
  Month 3+:  Obj 5 — Time series forecasting
  Month 4+:  Obj 8 — Recommendations (needs user system)

CONTINUOUSLY:
  Obj 9 — Dashboard additions after each model is built
  Obj 2 — Change detection already running, just accumulating
```

---

### Where LLMs/RAG/LangChain actually belong
```
Obj 3 anomaly detection    → NO LLM  (IsolationForest, sklearn)
Obj 4 multimodal           → NO LLM for detection, YES for explanation
Obj 5 forecasting          → NO LLM  (SARIMAX, Prophet, LSTM)
Obj 6 legal retrieval      → YES LangChain RAG  ← primary use case
Obj 7 contract generation  → YES LLM (Mistral) + RAG checker
Obj 8 recommendations      → LLM for query parsing only
Obj 9 reports              → LLM for narrative generation (optional)
The rule: LLMs where you need language understanding or generation. Classical ML everywhere else.

Immediate next step
Build modeling/anomaly/detector.py — Isolation Forest on your current 842 model-ready records. It needs no extra data, teaches you the modeling infrastructure, and its output (anomaly scores) feeds directly into the dashboard and reliability scorer.EstateMind — TDSP Progress Recap

What is TDSP
Microsoft's Team Data Science Process. Five phases:
1. Business Understanding    ✓ DONE
2. Data Acquisition          ✓ DONE  
3. Modeling                  ← YOU ARE HERE
4. Deployment                → next
5. Customer Acceptance       → future

Phase 1 — Business Understanding ✓
Nine objectives defined and mapped to technical solutions. Architecture designed. Storage strategy decided (Pinecone + SQLite).

Phase 2 — Data Acquisition & Understanding ✓
✓ 10 scrapers live (8 working, 2 partial — tecnocasa/tunisieannonce)
✓ Railway deployment — runs every 24h without laptop
✓ Pinecone vector DB — 969 vectors and growing
✓ Preprocessing pipeline — all 9 steps working:
    normalize → null_handler → dedup → outlier_flag → score → change_detect → upsert → export
✓ SQLite time series DB — price_history table recording changes
✓ Django + React dashboard — live, reads from Pinecone
✓ Reliability scoring — 0-100 per listing
✓ CSV exports — clean snapshot + model-ready snapshot daily
Current data quality snapshot:
Total vectors:     969 (growing daily)
Model-ready:       842 (87%)
HIGH quality:      134 (13.8%)
GOOD quality:       29 (3.0%)
LOW quality:       679 (70.0%)  ← surface/rooms still mostly null
DROP:              127 (13.1%)
Cross-source dups:   6 (0.6%)
Outliers:           62 (6.4%)
LOW is high because surface/rooms are missing from most listings — the NLP extractor will fix this once working. This will improve weekly as more data accumulates.

Phase 3 — Modeling (current phase)
Here is every objective mapped to its technical approach, ordered by dependency and feasibility:

Obj 3 — Anomaly Detection ← START HERE
What: flag statistically abnormal listings at regional level (not just rule-based outliers — learned patterns).
Why first: needs least data, teaches you the modeling infrastructure, feeds directly into the reliability score and dashboard.
Stack:
python# No LLM needed — pure sklearn
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

features = ["price", "surface", "rooms", "price_per_m2", 
            "image_count", "description_length"]
# Train per region + property_type group
# Output: anomaly_score per listing → store in Pinecone metadata
```

**File:** `modeling/anomaly/detector.py`

**Input:** `data/exports/model_ready_*.csv`
**Output:** `anomaly_score` and `anomaly_label` added to Pinecone metadata

**Data needed:** 200+ listings per region — you have enough for Tunis now.

---

#### Obj 2 — Change Detection ← ALREADY BUILT, needs activation

**What:** built in `change_detector.py`, writing to SQLite. Just needs to run for 2-3 weeks to accumulate meaningful history.

**Nothing to build** — just let the pipeline run daily. Come back to this for Obj 5.

---

#### Obj 1 — ETL Pipeline ← DONE

Already built and running. The preprocessing pipeline IS the ETL pipeline. Obj 1 is complete.

---

#### Obj 8 — Recommendation System ← needs frontend first

**What:** personalized property recommendations based on search history and user behavior.

**Stack:**
```
User behavior data → collaborative filtering OR content-based
Content-based:  user query vector → Pinecone similarity search (already works)
Collaborative:  needs user accounts + search logs → build after frontend
LLM role here: parse natural language user query into structured filters before Pinecone search.
python# Example flow
user_query = "appartement 3 chambres lac 2 budget 500k"
# → LLM extracts: {rooms: 3, city: "Lac 2", price_max: 500000, type: "Apartment"}
# → Pinecone query with metadata filters
# → ranked results
Blocked on: user accounts, search history logging. Build the frontend user system first.

Obj 5 — Time Series Price Forecasting ← needs 3-6 months data
What: multivariate forecasting of property prices. 1, 3, 5-year forecasts by region.
Stack:
python# Option A — classical (works with less data)
from statsmodels.tsa.statespace.sarimax import SARIMAX
# or
from prophet import Prophet  # Facebook Prophet

# Option B — deep learning (needs 1+ year of data)
# LSTM or Transformer (PyTorch)

# Exogenous variables (macroeconomic indicators):
# - Tunisia inflation rate
# - USD/TND exchange rate  
# - Construction permits issued
# - Population migration data
# → scrape from BCT (Banque Centrale de Tunisie) or INS
Data needed: minimum 90 days of daily price snapshots per region. You have ~5 days now. Come back in 3 months.
File: modeling/forecasting/price_forecast.py

Obj 4 — Multimodal Consistency Classifier ← needs image embeddings
What: detect semantic mismatches — photo shows a villa but description says studio, or price is 50k but listing claims luxury.
Stack:
python# Step 1: embed listing images using CLIP
from transformers import CLIPModel, CLIPProcessor
image_vector = clip.encode_image(image_url)  # 512-dim

# Step 2: store in Pinecone namespace "images"
# Step 3: compare image_vector vs text_vector → cosine distance
# Step 4: train classifier on (image_vec, text_vec, price) → mismatch_score

# LLM role: generate explanation of WHY it's mismatched
Blocked on: need to download and embed listing images. Currently only storing URLs. Build an image embedding pipeline first.
File: modeling/multimodal/consistency.py

Obj 6 & 7 — Legal RAG Pipeline ← LangChain goes here
What:

Obj 6: retrieve relevant legal clauses from Tunisian real estate law documents
Obj 7: LLM generates a contract → Legal module checks each clause against law → flags issues

This is the primary use case for LangChain and LLMs.
python# Stack
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Pinecone  # namespace: "legal_docs"
from langchain.chains import RetrievalQA
from langchain_community.llms import Ollama  # or OpenRouter

# Pipeline
# 1. Chunk legal documents (already have in PostgreSQL — migrate to Pinecone)
# 2. Embed chunks → Pinecone namespace "legal_docs"
# 3. User asks: "what are mandatory clauses for a sale contract in Tunisia?"
# 4. RAG retrieves relevant law chunks
# 5. LLM generates answer grounded in retrieved law

# Obj 7 adds:
# 6. LLM generates full contract from user inputs
# 7. Legal checker: for each clause, RAG-check against law
# 8. Output: contract + flagged issues + missing mandatory clauses
```

**LLM options (free/cheap):**
```
Mistral 7B via Ollama    → runs locally, free, good French
mistralai/mistral-7b via OpenRouter → ~$0.06/1M tokens
claude-haiku via Anthropic API → fast, cheap
```

**Blocked on:** migrating legal documents from PostgreSQL to Pinecone (new namespace).

---

#### Obj 9 — Analytics Dashboard ← built incrementally

**What:** multi-audience interactive dashboard with real-time monitoring, anomaly tracking, investment scoring, exportable reports.

**Current state:** basic dashboard exists (MetricsGrid, EDADashboard, DashboardTabs). Needs:
```
Add to React:
  - Anomaly alerts panel         ← after Obj 3
  - Price trend charts           ← after Obj 5 has data
  - Investment scoring map       ← after Obj 3 + scoring
  - Report export (PDF)          ← after modeling
  - Legal contract generator UI  ← after Obj 6/7
```

---

### Recommended build order
```
NOW (you have enough data):
  Week 1-2:  Obj 3 — Anomaly detection model
  Week 2-3:  Obj 4 setup — image embedding pipeline
  Week 3-4:  Obj 6/7 — Legal RAG (migrate PostgreSQL docs → Pinecone)

WAIT FOR DATA (3 months):
  Month 3+:  Obj 5 — Time series forecasting
  Month 4+:  Obj 8 — Recommendations (needs user system)

CONTINUOUSLY:
  Obj 9 — Dashboard additions after each model is built
  Obj 2 — Change detection already running, just accumulating
```

---

### Where LLMs/RAG/LangChain actually belong
```
Obj 3 anomaly detection    → NO LLM  (IsolationForest, sklearn)
Obj 4 multimodal           → NO LLM for detection, YES for explanation
Obj 5 forecasting          → NO LLM  (SARIMAX, Prophet, LSTM)
Obj 6 legal retrieval      → YES LangChain RAG  ← primary use case
Obj 7 contract generation  → YES LLM (Mistral) + RAG checker
Obj 8 recommendations      → LLM for query parsing only
Obj 9 reports              → LLM for narrative generation (optional)
The rule: LLMs where you need language understanding or generation. Classical ML everywhere else.

Immediate next step
Build modeling/anomaly/detector.py — Isolation Forest on your current 842 model-ready records. It needs no extra data, teaches you the modeling infrastructure, and its output (anomaly scores) feeds directly into the dashboard and reliability scorer.