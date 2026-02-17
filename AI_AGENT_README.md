# 🤖 Intelligent Scraping Agent System

Transform your simple scraper into an **autonomous AI agent** that makes intelligent decisions, self-heals, and runs continuously.

## 🌟 What Makes This an "AI Agent"?

### Traditional Scraper vs AI Agent

| Traditional Scraper | AI Agent |
|---------------------|----------|
| ❌ Runs when you tell it | ✅ Decides when to run |
| ❌ Fixed configuration | ✅ Adapts strategy based on conditions |
| ❌ Breaks and stops | ✅ Self-heals and continues |
| ❌ No learning | ✅ Learns from past performance |
| ❌ Manual monitoring | ✅ Autonomous monitoring |
| ❌ Static behavior | ✅ Dynamic decision-making |

## 🧠 Agent Intelligence Features

### 1. **Autonomous Decision-Making**
The agent chooses its scraping strategy based on:
- Current error rate
- Data quality scores
- Historical performance
- Website response times

```python
# Agent decides on its own:
if error_rate > 30%:
    strategy = CONSERVATIVE  # Slow down
elif data_quality < 80%:
    strategy = MINIMAL  # Just check updates
elif success_rate > 90%:
    strategy = AGGRESSIVE  # Speed up
else:
    strategy = BALANCED  # Normal operation
```

### 2. **Self-Healing Capabilities**
When issues detected, agent automatically:
- Analyzes the problem
- Attempts repair strategies
- Adjusts behavior
- Continues operation

```python
# Detects anomalies and self-heals:
anomaly_detected = "Only 5 listings found (expected >10)"
agent.self_heal(anomaly_detected)
# Agent investigates and adapts
```

### 3. **Adaptive Strategies**

**Aggressive Strategy:**
- 20 pages per run
- 1-second delays
- Used when: Success rate > 90%

**Balanced Strategy:**
- 10 pages per run
- 2-second delays  
- Used when: Normal conditions

**Conservative Strategy:**
- 5 pages per run
- 5-second delays
- Used when: Error rate > 30%

**Minimal Strategy:**
- 1 page per run
- 3-second delays
- Used when: Data quality < 80%

### 4. **Continuous Learning**
Agent tracks:
- Success/failure patterns
- Performance metrics over time
- Data quality trends
- Self-adjusts thresholds

## 📦 System Components

```
┌─────────────────────────────────────────────────────┐
│              AGENT SYSTEM ARCHITECTURE              │
└─────────────────────────────────────────────────────┘

┌──────────────────────┐
│  agent_scheduler.py  │ ← Runs agent on schedule
└──────────┬───────────┘
           │
           ▼
┌────────────────────────────┐
│ intelligent_scraping       │ ← Core AI agent
│ _agent.py                  │   - Makes decisions
└────────────┬───────────────┘   - Self-heals
             │                    - Adapts strategy
             │
             ├─────────────────────────────┐
             │                             │
             ▼                             ▼
    ┌────────────────┐          ┌──────────────────┐
    │ agent_data.db  │          │ agent_dashboard  │
    │                │          │ .py              │
    │ • Listings     │          │                  │
    │ • Metrics      │          │ Monitor & Control│
    │ • Sessions     │          └──────────────────┘
    └────────────────┘
```

## 🚀 Quick Start

### Installation

```bash
# Install dependencies
pip install requests beautifulsoup4 lxml pandas schedule

# You're ready!
```

### Run Agent Once

```bash
python intelligent_scraping_agent.py
```

**What happens:**
1. Agent analyzes current conditions
2. Decides optimal strategy
3. Executes scraping task
4. Saves to database
5. Reports metrics

### Run Agent Continuously

```bash
python agent_scheduler.py
```

**Choose option:**
- **Option 1:** Run once now
- **Option 2:** Continuous scheduled mode

**Default Schedule:**
- Daily scrape: 3:00 AM
- Quick check: Every 6 hours
- Full scrape: Sunday 2:00 AM

### Monitor with Dashboard

```bash
python agent_dashboard.py
```

**Dashboard shows:**
- Real-time metrics
- Recent sessions
- Database statistics
- Data quality
- Control options

## 📊 Understanding Agent Behavior

### Decision Flow

```
START
  │
  ▼
Analyze Conditions
  ├─ Error rate?
  ├─ Data quality?
  ├─ Success history?
  └─ Response times?
  │
  ▼
Choose Strategy
  ├─ AGGRESSIVE (if performing well)
  ├─ BALANCED (normal)
  ├─ CONSERVATIVE (if errors)
  └─ MINIMAL (if quality issues)
  │
  ▼
Create Task
  ├─ Set page count
  ├─ Set delays
  └─ Set priority
  │
  ▼
Execute Task
  ├─ Scrape pages
  ├─ Detect anomalies
  └─ Self-heal if needed
  │
  ▼
Save & Learn
  ├─ Update database
  ├─ Save metrics
  └─ Adjust for next time
  │
  ▼
END
```

### Anomaly Detection

Agent detects:
- **Too few listings** (< 10 per page)
- **Low quality data** (< 70% quality score)
- **Unusual prices** (too high/low)
- **Missing fields** (incomplete data)

### Self-Healing Actions

When anomaly detected:
1. **Log the issue**
2. **Pause briefly**
3. **Analyze HTML structure** (if needed)
4. **Adjust extraction logic** (if needed)
5. **Recalibrate thresholds** (if needed)
6. **Resume operation**

## 💾 Database Schema

### Tables

**listings**
- Stores all scraped listings
- Deduplicates automatically
- Tracks data quality

**agent_metrics**
- Performance over time
- Success/failure rates
- Quality scores

**scraping_sessions**
- Each scraping run
- Pages/listings found
- Errors encountered

### Sample Queries

```python
# Get all listings from Tunis
SELECT * FROM listings WHERE region = 'Tunis'

# Average price by region
SELECT region, AVG(price) FROM listings GROUP BY region

# Success rate over time
SELECT 
    DATE(start_time) as date,
    COUNT(*) as total,
    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful
FROM scraping_sessions
GROUP BY DATE(start_time)
```

## 🎮 Using the Dashboard

### Main Screen

```
================================================================================
                    🤖 AGENT MONITORING DASHBOARD
================================================================================
Time: 2026-02-16 10:30:45
Database: agent_data.db
--------------------------------------------------------------------------------

📊 AGENT METRICS
--------------------------------------------------------------------------------
Total Listings Scraped:  1,247
Total Pages Scraped:     53
Successful Scrapes:      12
Failed Scrapes:          1
Error Rate:              7.7%
Data Quality Score:      92.3%
Self-Heals Performed:    2
Average Scrape Time:     45.3s
Last Scrape:             2026-02-16T10:15:32

📋 RECENT SCRAPING SESSIONS
--------------------------------------------------------------------------------
      start                  end        pages  listings  errors  strategy  status
2026-02-16 10:15:30  2026-02-16 10:16:15   10       95      0  balanced  SUCCESS
2026-02-16 04:00:15  2026-02-16 04:02:30   20      187      1  aggressive SUCCESS
...
```

### Menu Options

1. **Database Statistics** - See total listings, regional breakdown, prices
2. **Data Quality** - Check completeness, quality scores
3. **Export Data** - Export all, recent, or by region
4. **Control Agent** - Run manually, view logs
5. **Refresh** - Update dashboard
6. **Exit** - Close dashboard

## 🔧 Configuration

### Modify Agent Behavior

Edit `intelligent_scraping_agent.py`:

```python
class IntelligentScrapingAgent:
    def __init__(self):
        # Thresholds for decision-making
        self.error_threshold = 0.3  # 30% error triggers conservative mode
        self.min_listings_per_page = 10  # Below this is anomaly
        self.max_retries = 3  # Retry failed requests 3 times
```

### Modify Schedule

Edit `agent_scheduler.py`:

```python
def schedule_tasks(self):
    # Daily at 3 AM
    schedule.every().day.at("03:00").do(self.run_agent_cycle)
    
    # Every 6 hours
    schedule.every(6).hours.do(self.run_agent_cycle)
    
    # Add your own:
    schedule.every().monday.at("09:00").do(self.run_agent_cycle)
    schedule.every(2).hours.do(self.run_agent_cycle)
```

### Modify Strategies

Edit strategy configurations:

```python
strategy_configs = {
    ScrapingStrategy.AGGRESSIVE: ScrapingTask(
        max_pages=20,  # ← Change this
        delay=1,       # ← Change this
        strategy=strategy,
        priority=3
    ),
    # ... etc
}
```

## 📈 Advanced Usage

### Run Agent Programmatically

```python
from intelligent_scraping_agent import IntelligentScrapingAgent

# Create agent
agent = IntelligentScrapingAgent()

# Run autonomous cycle
agent.run_autonomous_cycle()

# Or control manually
from intelligent_scraping_agent import ScrapingTask, ScrapingStrategy

task = ScrapingTask(
    max_pages=15,
    delay=2,
    strategy=ScrapingStrategy.BALANCED
)

agent.execute_task(task)

# Export data
agent.export_data("my_export.csv")
```

### Query Database

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('agent_data.db')

# Get recent listings
df = pd.read_sql_query('''
    SELECT * FROM listings
    WHERE scraped_at >= datetime('now', '-7 days')
    ORDER BY price DESC
''', conn)

print(df.head())

conn.close()
```

### Monitor Logs

```bash
# Watch live logs
tail -f agent.log

# Search for errors
grep ERROR agent.log

# Count successful scrapes
grep "TASK COMPLETE" agent.log | wc -l
```

## 🔍 Troubleshooting

### Problem: Agent keeps using CONSERVATIVE strategy

**Cause:** High error rate

**Solution:**
1. Check `agent.log` for errors
2. Run `html_analyzer_clean.py` to verify site structure
3. Reduce `error_threshold` if too sensitive

### Problem: Low data quality scores

**Cause:** Incomplete data extraction

**Solution:**
1. Check recent listings in database
2. Verify field mapping is correct
3. Adjust `_calculate_quality_score()` weights

### Problem: Agent not running on schedule

**Cause:** Scheduler not running

**Solution:**
```bash
# Make sure scheduler is running
python agent_scheduler.py

# Check scheduler logs
tail scheduler.log
```

### Problem: Database growing too large

**Solution:**
```python
# Clean old data
import sqlite3

conn = sqlite3.connect('agent_data.db')
cursor = conn.cursor()

# Delete listings older than 6 months
cursor.execute('''
    DELETE FROM listings
    WHERE scraped_at < datetime('now', '-6 months')
''')

conn.commit()
conn.close()
```

## 🎯 Real-World Scenarios

### Scenario 1: Daily Price Monitoring

**Goal:** Track price changes for investment analysis

**Setup:**
```python
# In agent_scheduler.py
schedule.every().day.at("06:00").do(self.run_agent_cycle)

# Query price changes
SELECT 
    region,
    AVG(price) as avg_price,
    DATE(scraped_at) as date
FROM listings
GROUP BY region, DATE(scraped_at)
ORDER BY date, region
```

### Scenario 2: Market Research

**Goal:** Understand regional supply/demand

**Setup:**
```python
# Weekly full scrape
schedule.every().sunday.at("02:00").do(self.run_full_scrape)

# Analysis
SELECT 
    region,
    nature,
    COUNT(*) as listings_count,
    AVG(price) as avg_price
FROM listings
WHERE scraped_at >= datetime('now', '-7 days')
GROUP BY region, nature
```

### Scenario 3: Competitive Analysis

**Goal:** Monitor specific property types

**Setup:**
```python
# Filter in export
conn = sqlite3.connect('agent_data.db')
df = pd.read_sql_query('''
    SELECT * FROM listings
    WHERE type = 'Appartement'
    AND price BETWEEN 200000 AND 500000
''', conn)

# Analyze
print(df.groupby('region')['price'].describe())
```

## 🚀 Next Steps

### Enhancements to Add

1. **Email Notifications**
```python
import smtplib

def send_alert(subject, message):
    # Send email when anomaly detected
    pass
```

2. **Slack Integration**
```python
import requests

def post_to_slack(metrics):
    webhook_url = "YOUR_WEBHOOK"
    requests.post(webhook_url, json={"text": metrics})
```

3. **Price Change Alerts**
```python
def detect_price_changes():
    # Compare with previous scrape
    # Alert on significant changes
    pass
```

4. **Machine Learning Enhancement**
```python
from sklearn.ensemble import RandomForestRegressor

def predict_listing_quality(features):
    # ML model to predict quality
    pass
```

## 📚 Architecture Patterns

This agent implements several AI/ML patterns:

- **Autonomous Agent Pattern** - Self-directed behavior
- **Self-Healing Pattern** - Auto-recovery from failures
- **Adaptive Learning Pattern** - Adjusts based on experience
- **Observer Pattern** - Monitors own performance
- **Strategy Pattern** - Switches behavior dynamically

## 🎓 What You've Built

You've created a **production-grade autonomous system** that:

✅ Makes intelligent decisions  
✅ Adapts to changing conditions  
✅ Heals itself when problems occur  
✅ Runs continuously without supervision  
✅ Learns from past performance  
✅ Stores data persistently  
✅ Provides monitoring dashboard  

**This is NOT just a scraper — it's an intelligent agent system!**

---

## 📞 Summary

Your agent system has 3 main components:

1. **intelligent_scraping_agent.py** - The brain (makes decisions)
2. **agent_scheduler.py** - The manager (runs on schedule)
3. **agent_dashboard.py** - The monitor (visualize & control)

**Start simple:**
```bash
python intelligent_scraping_agent.py  # Run once
```

**Then automate:**
```bash
python agent_scheduler.py  # Continuous operation
```

**Monitor:**
```bash
python agent_dashboard.py  # View performance
```

---

**Version:** 1.0  
**Last Updated:** February 2026  
**Your achievement:** You went from basic scraper to AI agent! 🎉
