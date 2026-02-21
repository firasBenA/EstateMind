"""
Intelligent Real Estate Scraping Agent - PostgreSQL Only
=========================================================
Pure PostgreSQL implementation using existing PostgresClient

Features:
- Autonomous decision-making
- Self-healing capabilities
- Adaptive scraping strategies
- PostgreSQL database (production-ready)
- No SQLite dependencies
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

from postgres_client import PostgresClient


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('agent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AgentState(Enum):
    """Agent operational states"""
    IDLE = "idle"
    SCRAPING = "scraping"
    ANALYZING = "analyzing"
    HEALING = "healing"
    ERROR = "error"


class ScrapingStrategy(Enum):
    """Scraping strategies"""
    AGGRESSIVE = "aggressive"      # Fast, many pages
    BALANCED = "balanced"          # Normal operation
    CONSERVATIVE = "conservative"  # Slow, careful
    MINIMAL = "minimal"           # Just check for updates


@dataclass
class AgentMetrics:
    """Agent performance metrics"""
    total_listings_scraped: int = 0
    successful_scrapes: int = 0
    failed_scrapes: int = 0
    total_pages_scraped: int = 0
    average_scrape_time: float = 0.0
    last_scrape_time: Optional[str] = None
    errors_encountered: int = 0
    self_heals_performed: int = 0
    data_quality_score: float = 100.0
    
    def to_dict(self):
        return asdict(self)


@dataclass
class ScrapingTask:
    """Task definition"""
    max_pages: int
    delay: int
    strategy: ScrapingStrategy
    priority: int = 1


class IntelligentScrapingAgent:
    """
    Autonomous agent for intelligent web scraping
    Uses PostgreSQL for all data storage
    """
    
    def __init__(self):
        """Initialize the intelligent agent"""
        self.state = AgentState.IDLE
        self.metrics = AgentMetrics()
        
        # Initialize PostgreSQL client
        self.pg = PostgresClient()
        logger.info("✅ PostgreSQL client connected")
        
        # Agent configuration
        self.base_url = "http://www.tunisie-annonce.com/AnnoncesImmobilier.asp"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Intelligent thresholds
        self.error_threshold = 0.3  # 30% error rate triggers healing
        self.min_listings_per_page = 10
        self.max_retries = 3
        
        # Create metrics table if needed
        self._init_metrics_table()
        
        # Load previous metrics
        self._load_metrics()
        
        logger.info("🤖 Intelligent Scraping Agent initialized")
        logger.info(f"   State: {self.state.value}")
        logger.info(f"   Database: estatemind (PostgreSQL)")
    
    def _init_metrics_table(self):
        """Create agent_metrics table in PostgreSQL"""
        try:
            with self.pg.conn.cursor() as cur:
                # Agent metrics table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS agent_metrics (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        metric_name TEXT,
                        metric_value DOUBLE PRECISION
                    )
                """)
                
                # Scraping sessions table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS scraping_sessions (
                        id SERIAL PRIMARY KEY,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        pages_scraped INTEGER,
                        listings_found INTEGER,
                        errors_count INTEGER,
                        strategy TEXT,
                        success BOOLEAN
                    )
                """)
            
            self.pg.conn.commit()
            logger.info("✅ Metrics tables initialized")
        except Exception as e:
            self.pg.conn.rollback()
            logger.warning(f"Could not init metrics tables: {e}")
    
    def _load_metrics(self):
        """Load previous metrics from PostgreSQL"""
        try:
            with self.pg.conn.cursor() as cur:
                cur.execute("""
                    SELECT metric_name, metric_value 
                    FROM agent_metrics 
                    WHERE timestamp = (SELECT MAX(timestamp) FROM agent_metrics)
                """)
                
                for metric_name, metric_value in cur.fetchall():
                    if hasattr(self.metrics, metric_name):
                        setattr(self.metrics, metric_name, metric_value)
            
            logger.info("📊 Previous metrics loaded from PostgreSQL")
        except Exception as e:
            logger.debug(f"Could not load previous metrics: {e}")
    
    def _save_metrics(self):
        """Save current metrics to PostgreSQL"""
        try:
            with self.pg.conn.cursor() as cur:
                timestamp = datetime.now()
                
                for metric_name, metric_value in self.metrics.to_dict().items():
                    if isinstance(metric_value, (int, float)):
                        cur.execute("""
                            INSERT INTO agent_metrics (timestamp, metric_name, metric_value)
                            VALUES (%s, %s, %s)
                        """, (timestamp, metric_name, metric_value))
            
            self.pg.conn.commit()
        except Exception as e:
            self.pg.conn.rollback()
            logger.error(f"Error saving metrics: {e}")
    
    def decide_strategy(self) -> ScrapingStrategy:
        """
        Intelligently decide scraping strategy based on conditions
        
        Returns:
            ScrapingStrategy: Chosen strategy
        """
        logger.info("🧠 Making strategic decision...")
        
        # Calculate error rate
        total_scrapes = self.metrics.successful_scrapes + self.metrics.failed_scrapes
        error_rate = self.metrics.failed_scrapes / total_scrapes if total_scrapes > 0 else 0
        
        # Decision logic
        if error_rate > self.error_threshold:
            strategy = ScrapingStrategy.CONSERVATIVE
            reason = f"High error rate ({error_rate:.1%})"
        elif self.metrics.data_quality_score < 80:
            strategy = ScrapingStrategy.MINIMAL
            reason = f"Low data quality ({self.metrics.data_quality_score:.1f}%)"
        elif self.metrics.successful_scrapes > 10 and error_rate < 0.1:
            strategy = ScrapingStrategy.AGGRESSIVE
            reason = f"Good performance (success rate: {1-error_rate:.1%})"
        else:
            strategy = ScrapingStrategy.BALANCED
            reason = "Normal conditions"
        
        logger.info(f"   Decision: {strategy.value.upper()} ({reason})")
        return strategy
    
    def create_task_from_strategy(self, strategy: ScrapingStrategy) -> ScrapingTask:
        """Create scraping task based on strategy"""
        strategy_configs = {
            ScrapingStrategy.AGGRESSIVE: ScrapingTask(20, 1, strategy, 3),
            ScrapingStrategy.BALANCED: ScrapingTask(10, 2, strategy, 2),
            ScrapingStrategy.CONSERVATIVE: ScrapingTask(5, 5, strategy, 1),
            ScrapingStrategy.MINIMAL: ScrapingTask(1, 3, strategy, 1)
        }
        return strategy_configs[strategy]
    
    def fetch_page(self, page_num: int, timeout: int = 15) -> Optional[str]:
        """Fetch page with intelligent retry logic"""
        for attempt in range(self.max_retries):
            try:
                url = f"{self.base_url}?rech_page_num={page_num}"
                response = requests.get(url, headers=self.headers, timeout=timeout)
                
                if response.status_code == 200:
                    return response.text
                else:
                    logger.warning(f"HTTP {response.status_code} for page {page_num}, attempt {attempt+1}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on page {page_num}, attempt {attempt+1}")
            except Exception as e:
                logger.error(f"Error fetching page {page_num}: {e}")
            
            # Exponential backoff
            if attempt < self.max_retries - 1:
                wait_time = 2 ** attempt
                time.sleep(wait_time)
        
        return None
    
    def parse_listings(self, html: str) -> List[Dict]:
        """Parse listings from HTML"""
        soup = BeautifulSoup(html, 'lxml')
        listings = []
        
        header_row = soup.find('tr', class_='Entete1')
        if not header_row:
            return listings
        
        main_table = header_row.find_parent('table')
        if not main_table:
            return listings
        
        rows = main_table.find_all('tr')
        header_found = False
        
        for row in rows:
            if not header_found:
                if row == header_row:
                    header_found = True
                continue
            
            listing = self._extract_listing(row)
            if listing:
                listings.append(listing)
        
        return listings
    
    def _extract_listing(self, row) -> Optional[Dict]:
        """Extract and validate single listing"""
        try:
            cells = row.find_all('td')
            content_cells = [cell for cell in cells if cell.get_text(strip=True)]
            
            if len(content_cells) < 5:
                return None
            
            link = row.find('a')
            url = link['href'] if link and 'href' in link.attrs else None
            if url and not url.startswith('http'):
                url = f"http://www.tunisie-annonce.com/{url}"
            
            listing = {
                'region': self._clean_text(content_cells[0].get_text(strip=True)),
                'nature': self._clean_text(content_cells[1].get_text(strip=True)),
                'type': self._clean_text(content_cells[2].get_text(strip=True)),
                'description': self._clean_text(content_cells[3].get_text(strip=True)),
                'price_text': self._clean_text(content_cells[4].get_text(strip=True)),
                'date_modified': self._clean_text(content_cells[5].get_text(strip=True)),
                'url': url,
                'scraped_at': datetime.now().isoformat()
            }
            
            listing['price'] = self._extract_price(listing['price_text'])
            listing['listing_id'] = self._generate_listing_id(listing)
            listing['data_quality_score'] = self._calculate_quality_score(listing)
            
            if self._validate_listing(listing):
                return listing
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting listing: {e}")
            return None
    
    def _clean_text(self, text: str) -> Optional[str]:
        """Clean text"""
        if not text:
            return None
        text = re.sub(r'\s+', ' ', text).strip()
        return text if text else None
    
    def _extract_price(self, price_text: str) -> Optional[int]:
        """Extract numeric price"""
        if not price_text:
            return None
        
        price_text = price_text.replace(' ', '').replace(',', '').replace('.', '')
        numbers = re.findall(r'\d+', price_text)
        
        if numbers:
            try:
                price = int(''.join(numbers))
                if 1000 < price < 100000000:
                    return price
            except ValueError:
                pass
        
        return None
    
    def _generate_listing_id(self, listing: Dict) -> str:
        """Generate unique listing ID"""
        if listing.get('url'):
            return listing['url'].split('=')[-1] if '=' in listing['url'] else listing['url']
        
        import hashlib
        key = f"{listing.get('region')}_{listing.get('description', '')}_{listing.get('price')}"
        return hashlib.md5(key.encode()).hexdigest()[:16]
    
    def _calculate_quality_score(self, listing: Dict) -> float:
        """Calculate data quality score"""
        score = 0.0
        max_score = 0.0
        
        fields = {
            'region': 15,
            'nature': 10,
            'type': 10,
            'description': 30,
            'price': 20,
            'date_modified': 10,
            'url': 5
        }
        
        for field, weight in fields.items():
            max_score += weight
            if listing.get(field):
                score += weight
        
        return (score / max_score) * 100 if max_score > 0 else 0
    
    def _validate_listing(self, listing: Dict) -> bool:
        """Validate listing quality"""
        if not listing.get('description') or len(listing['description']) < 10:
            return False
        
        if listing.get('data_quality_score', 0) < 50:
            return False
        
        return True
    
    def detect_anomalies(self, listings: List[Dict]) -> Tuple[bool, str]:
        """Detect anomalies in scraped data"""
        if not listings:
            return True, "No listings found"
        
        if len(listings) < self.min_listings_per_page:
            return True, f"Only {len(listings)} listings (expected >{self.min_listings_per_page})"
        
        avg_quality = sum(l.get('data_quality_score', 0) for l in listings) / len(listings)
        if avg_quality < 70:
            return True, f"Low average quality: {avg_quality:.1f}%"
        
        prices = [l['price'] for l in listings if l.get('price')]
        if prices:
            avg_price = sum(prices) / len(prices)
            if avg_price < 5000 or avg_price > 5000000:
                return True, f"Unusual average price: {avg_price:,.0f} TND"
        
        return False, "Data looks normal"
    
    def self_heal(self, issue: str):
        """Self-healing mechanism"""
        self.state = AgentState.HEALING
        logger.warning(f"🔧 Self-healing triggered: {issue}")
        
        self.metrics.self_heals_performed += 1
        
        time.sleep(5)
        self.state = AgentState.IDLE
        logger.info("   Self-healing complete")
    
    def save_to_database(self, listings: List[Dict]):
        """Save listings to PostgreSQL using your existing schema"""
        new_or_updated = 0
        
        for listing in listings:
            try:
                # Map to your PostgreSQL schema
                payload = self._map_listing_to_postgres(listing)
                
                # Use your PostgresClient upsert method
                status = self.pg.upsert_listing(payload)
                
                if status != "error":
                    new_or_updated += 1
                    
            except Exception as e:
                logger.error(f"Error saving listing: {e}")
        
        logger.info(f"💾 PostgreSQL: {new_or_updated} listings upserted")
    
    def _map_listing_to_postgres(self, listing: Dict) -> Dict:
        """
        Map scraped listing to your PostgreSQL schema
        
        Your schema expects:
        - source_id, source_name, url, title, description
        - price, currency, property_type, transaction_type
        - location (dict), images (list), features (dict), pois (list)
        - surface_area_m2, rooms, scraped_at, last_update
        """
        # Parse region
        region_text = listing.get("region") or ""
        
        # Build location dict
        location = {
            "governorate": region_text,
            "city": region_text,
            "district": None,
            "latitude": None,
            "longitude": None,
        }
        
        # Determine transaction type from nature
        nature = (listing.get("nature") or "").lower()
        transaction_type = "Sale"
        if any(x in nature for x in ["louer", "location", "rent"]):
            transaction_type = "Rent"
        
        # Property type
        property_type = (listing.get("type") or "").title() or "Other"
        
        # Build payload matching your PostgresClient schema
        return {
            "source_id": listing.get("listing_id"),
            "source_name": "tunisieannonce",
            "url": listing.get("url"),
            "title": (listing.get("description") or "")[:80] or listing.get("url", "No title"),
            "description": listing.get("description"),
            "price": listing.get("price"),
            "currency": "TND",
            "property_type": property_type,
            "transaction_type": transaction_type,
            "location": location,
            "images": [],
            "features": {
                "data_quality_score": listing.get("data_quality_score"),
                "price_text": listing.get("price_text")
            },
            "pois": [],
            "surface_area_m2": None,
            "rooms": None,
            "scraped_at": listing.get("scraped_at"),
            "last_update": listing.get("date_modified"),
            "raw_data_path": None,
        }
    
    def execute_task(self, task: ScrapingTask) -> bool:
        """Execute a scraping task"""
        self.state = AgentState.SCRAPING
        start_time = datetime.now()
        
        logger.info("=" * 70)
        logger.info(f"🚀 EXECUTING TASK: {task.strategy.value.upper()}")
        logger.info(f"   Pages: {task.max_pages}, Delay: {task.delay}s")
        logger.info("=" * 70)
        
        all_listings = []
        pages_scraped = 0
        errors = 0
        
        for page_num in range(1, task.max_pages + 1):
            # Fetch page
            html = self.fetch_page(page_num)
            
            if html is None:
                errors += 1
                self.metrics.failed_scrapes += 1
                logger.error(f"❌ Failed to fetch page {page_num}")
                continue
            
            # Parse listings
            listings = self.parse_listings(html)
            pages_scraped += 1
            
            if listings:
                # Detect anomalies
                has_anomaly, anomaly_msg = self.detect_anomalies(listings)
                
                if has_anomaly:
                    logger.warning(f"⚠️  Anomaly detected on page {page_num}: {anomaly_msg}")
                    self.self_heal(anomaly_msg)
                
                all_listings.extend(listings)
                logger.info(f"✅ Page {page_num}: {len(listings)} listings")
            else:
                logger.warning(f"⚠️  Page {page_num}: No listings found")
            
            # Delay before next page
            if page_num < task.max_pages:
                time.sleep(task.delay)
        
        # Save to PostgreSQL
        if all_listings:
            self.save_to_database(all_listings)
            self.metrics.successful_scrapes += 1
        else:
            self.metrics.failed_scrapes += 1
        
        # Update metrics
        self.metrics.total_listings_scraped += len(all_listings)
        self.metrics.total_pages_scraped += pages_scraped
        self.metrics.errors_encountered += errors
        self.metrics.last_scrape_time = datetime.now().isoformat()
        
        # Calculate average quality
        if all_listings:
            avg_quality = sum(l.get('data_quality_score', 0) for l in all_listings) / len(all_listings)
            self.metrics.data_quality_score = avg_quality
        
        # Save metrics
        self._save_metrics()
        
        # Log session
        self._log_session(start_time, pages_scraped, len(all_listings), errors, task.strategy, len(all_listings) > 0)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        self.metrics.average_scrape_time = duration
        
        logger.info("=" * 70)
        logger.info(f"✅ TASK COMPLETE")
        logger.info(f"   Duration: {duration:.1f}s")
        logger.info(f"   Listings: {len(all_listings)}")
        logger.info(f"   Pages: {pages_scraped}/{task.max_pages}")
        logger.info(f"   Errors: {errors}")
        logger.info("=" * 70)
        
        self.state = AgentState.IDLE
        return len(all_listings) > 0
    
    def _log_session(self, start_time, pages, listings, errors, strategy, success):
        """Log scraping session to PostgreSQL"""
        try:
            with self.pg.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scraping_sessions 
                    (start_time, end_time, pages_scraped, listings_found, errors_count, strategy, success)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (start_time, datetime.now(), pages, listings, errors, strategy.value, success))
            
            self.pg.conn.commit()
        except Exception as e:
            self.pg.conn.rollback()
            logger.error(f"Error logging session: {e}")
    
    def run_autonomous_cycle(self):
        """Run one autonomous decision-making cycle"""
        logger.info("\n🤖 Starting autonomous cycle...")
        
        # 1. Decide strategy
        strategy = self.decide_strategy()
        
        # 2. Create task
        task = self.create_task_from_strategy(strategy)
        
        # 3. Execute task
        success = self.execute_task(task)
        
        # 4. Report metrics
        self.report_metrics()
        
        return success
    
    def report_metrics(self):
        """Report current performance metrics"""
        logger.info("\n" + "=" * 70)
        logger.info("📊 AGENT PERFORMANCE METRICS")
        logger.info("=" * 70)
        logger.info(f"Total Listings: {self.metrics.total_listings_scraped}")
        logger.info(f"Total Pages: {self.metrics.total_pages_scraped}")
        logger.info(f"Successful Scrapes: {self.metrics.successful_scrapes}")
        logger.info(f"Failed Scrapes: {self.metrics.failed_scrapes}")
        
        if self.metrics.successful_scrapes + self.metrics.failed_scrapes > 0:
            error_rate = self.metrics.failed_scrapes / (self.metrics.successful_scrapes + self.metrics.failed_scrapes) * 100
            logger.info(f"Error Rate: {error_rate:.1f}%")
        
        logger.info(f"Data Quality: {self.metrics.data_quality_score:.1f}%")
        logger.info(f"Self-Heals: {self.metrics.self_heals_performed}")
        logger.info(f"Last Scrape: {self.metrics.last_scrape_time}")
        logger.info("=" * 70)
    
    def export_data(self, filename: str = None) -> str:
        """Export listings from PostgreSQL to CSV"""
        if filename is None:
            filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            with self.pg.conn.cursor() as cur:
                cur.execute("SELECT * FROM listings WHERE source_name = 'tunisieannonce' ORDER BY scraped_at DESC")
                
                # Get column names
                columns = [desc[0] for desc in cur.description]
                
                # Fetch all rows
                rows = cur.fetchall()
                
                # Create DataFrame
                df = pd.DataFrame(rows, columns=columns)
                
                # Save to CSV
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                
                logger.info(f"📤 Exported {len(df)} listings to {filename}")
                return filename
                
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            return None
    
    def close(self):
        """Close PostgreSQL connection"""
        try:
            self.pg.close()
            logger.info("PostgreSQL connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")


def main():
    """Main execution"""
    print("=" * 70)
    print("🤖 INTELLIGENT SCRAPING AGENT - POSTGRESQL")
    print("=" * 70)
    print()
    
    try:
        # Create agent
        agent = IntelligentScrapingAgent()
        
        # Run autonomous cycle
        agent.run_autonomous_cycle()
        
        # Export data
        agent.export_data()
        
        # Close connection
        agent.close()
        
        print("\n✅ Agent cycle complete!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    main()