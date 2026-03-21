
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
