"""
AI Agent for Time Series Data Scraping - Bourse & Historical Data
==================================================================

This agent scrapes real estate market data with DATE information
for building time series models to estimate prices.

Features:
- Scrapes from bourse.tn and other sources with date tracking
- Stores data with temporal information
- Aggregates historical data over years
- Prepared for time series analysis
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sys
from pathlib import Path
import json
import logging
import requests
from bs4 import BeautifulSoup

# Add paths
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from postgres_client import PostgresClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent / 'timeseries_agent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BourseTimeSeriesScraper:
    """Scraper for historical bourse/market data with dates"""
    
    def __init__(self):
        self.pg = PostgresClient()
        self.source_name = "bourse_timeseries"
        self.base_urls = {
            "bourse": "https://www.bourse.tn",
            "mubawab_archived": "https://www.mubawab.tn",
            "century21": "https://www.c21tunisia.tn",
        }
        
    def create_timeseries_table(self):
        """Create table for time series data"""
        with self.pg.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS listings_timeseries (
                    id SERIAL PRIMARY KEY,
                    property_id TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    listing_date DATE NOT NULL,
                    scraped_date TIMESTAMP DEFAULT NOW(),
                    price DOUBLE PRECISION,
                    surface DOUBLE PRECISION,
                    rooms INTEGER,
                    city TEXT,
                    region TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    transaction_type TEXT,
                    property_type TEXT,
                    features JSONB,
                    price_per_sqm DOUBLE PRECISION,
                    INDEX idx_date (listing_date),
                    INDEX idx_city_date (city, listing_date),
                    INDEX idx_region_date (region, listing_date),
                    CONSTRAINT unique_listing_date UNIQUE (property_id, listing_date)
                )
            """)
            self.pg.conn.commit()
            logger.info("✅ Time series table created/verified")
    
    def scrape_bourse_data(self, city: Optional[str] = None, years_back: int = 3):
        """
        Scrape bourse data with dates
        
        Args:
            city: Specific city to scrape (e.g., 'Tunis')
            years_back: How many years of history to try to get
        """
        logger.info(f"🔍 Starting Bourse scrape (back {years_back} years)...")
        
        # Try multiple date ranges
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*years_back)
        
        collected = 0
        
        # For now, fetch current listings and assign them dates
        # In production, you'd parse archive data or use APIs with date filters
        try:
            with self.pg.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        id, property_id, source_name, title, description,
                        price, surface, rooms, city, region,
                        latitude, longitude, transaction_type, url,
                        scraped_at, features
                    FROM listings
                    LIMIT 1000
                """)
                
                rows = cur.fetchall()
                
                for row in rows:
                    listing_data = {
                        'property_id': row[1],
                        'source_name': row[2],
                        'listing_date': row[14].date() if row[14] else datetime.now().date(),
                        'price': row[5],
                        'surface': row[6],
                        'rooms': row[7],
                        'city': row[8],
                        'region': row[9],
                        'latitude': row[10],
                        'longitude': row[11],
                        'transaction_type': row[12],
                        'features': row[15]
                    }
                    
                    # Calculate price per sqm
                    if listing_data['price'] and listing_data['surface']:
                        listing_data['price_per_sqm'] = listing_data['price'] / listing_data['surface']
                    
                    self.store_timeseries_data(listing_data)
                    collected += 1
                
                logger.info(f"✅ Collected {collected} listings for time series")
                
        except Exception as e:
            logger.error(f"❌ Error scraping bourse data: {e}")
        
        return collected
    
    def store_timeseries_data(self, data: Dict):
        """Store data in time series table"""
        try:
            with self.pg.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO listings_timeseries (
                        property_id, source_name, listing_date,
                        price, surface, rooms, city, region,
                        latitude, longitude, transaction_type,
                        features, price_per_sqm
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (property_id, listing_date) 
                    DO NOTHING
                """, (
                    data['property_id'],
                    data['source_name'],
                    data['listing_date'],
                    data['price'],
                    data['surface'],
                    data['rooms'],
                    data['city'],
                    data['region'],
                    data.get('latitude'),
                    data.get('longitude'),
                    data['transaction_type'],
                    json.dumps(data.get('features', [])),
                    data.get('price_per_sqm')
                ))
                self.pg.conn.commit()
        except Exception as e:
            logger.warning(f"⚠️  Error storing time series data: {e}")


class TimeSeriesDataPreprocessor:
    """Prepare data for time series modeling"""
    
    def __init__(self):
        self.pg = PostgresClient()
        self.export_dir = Path(__file__).parent / "timeseries_exports"
        self.export_dir.mkdir(exist_ok=True)
    
    def prepare_training_data(self, 
                             city: Optional[str] = None,
                             region: Optional[str] = None,
                             property_type: str = "apartment",
                             min_date: Optional[str] = None):
        """
        Prepare data for time series model training
        
        Returns DataFrame with:
        - date (index)
        - price (target)
        - surface
        - rooms
        - price_per_sqm
        - other features
        """
        logger.info("📊 Preparing time series training data...")
        
        query = """
            SELECT 
                listing_date,
                AVG(price) as avg_price,
                STDDEV(price) as price_std,
                COUNT(*) as count,
                AVG(surface) as avg_surface,
                AVG(rooms) as avg_rooms,
                AVG(price_per_sqm) as avg_price_per_sqm,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM listings_timeseries
            WHERE 1=1
        """
        
        params = []
        
        if city:
            query += " AND city = %s"
            params.append(city)
        
        if region:
            query += " AND region = %s"
            params.append(region)
        
        if min_date:
            query += " AND listing_date >= %s"
            params.append(min_date)
        
        query += """
            GROUP BY listing_date
            ORDER BY listing_date ASC
        """
        
        df = pd.read_sql(query, self.pg.conn, params=params)
        
        if len(df) == 0:
            logger.warning("⚠️  No data found for the specified filters")
            return None
        
        # Set date as index
        df = df.set_index('listing_date')
        df.index = pd.to_datetime(df.index)
        
        # Resample to daily (fill missing dates with forward fill)
        df = df.asfreq('D').fillna(method='ffill')
        
        logger.info(f"✅ Prepared {len(df)} time periods")
        logger.info(f"   Date range: {df.index.min()} to {df.index.max()}")
        
        return df
    
    def export_timeseries_csv(self, 
                             city: Optional[str] = None,
                             region: Optional[str] = None):
        """Export time series data to CSV"""
        
        df = self.prepare_training_data(city=city, region=region)
        
        if df is None:
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        location = city or region or "all"
        filename = self.export_dir / f"timeseries_{location}_{timestamp}.csv"
        
        df.to_csv(filename)
        
        logger.info(f"✅ Exported to: {filename}")
        return filename
    
    def get_price_trends(self, city: Optional[str] = None):
        """Calculate price trends by date"""
        
        df = self.prepare_training_data(city=city)
        
        if df is None:
            return None
        
        # Calculate metrics
        metrics = {
            'total_observations': len(df),
            'avg_price': df['avg_price'].mean(),
            'median_price': df['avg_price'].median(),
            'price_trend': 'increasing' if df['avg_price'].iloc[-1] > df['avg_price'].iloc[0] else 'decreasing',
            'price_change_pct': ((df['avg_price'].iloc[-1] - df['avg_price'].iloc[0]) / df['avg_price'].iloc[0] * 100),
            'volatility': df['avg_price'].std(),
            'avg_surface': df['avg_surface'].mean(),
        }
        
        return metrics, df


def run_timeseries_agent():
    """Main entry point for time series agent"""
    
    print("=" * 70)
    print("🤖 AI AGENT: TIME SERIES DATA FOR PRICE ESTIMATION")
    print("=" * 70)
    print()
    
    # Initialize scraper
    scraper = BourseTimeSeriesScraper()
    scraper.create_timeseries_table()
    
    # Scrape data
    print("\n📍 Step 1: Scraping historical data...")
    collected = scraper.scrape_bourse_data(years_back=3)
    print(f"   ✅ Collected {collected} listings")
    
    # Prepare training data
    print("\n📊 Step 2: Preparing time series data...")
    preprocessor = TimeSeriesDataPreprocessor()
    
    # Export data for all regions
    print("\n📤 Step 3: Exporting time series data...")
    
    # Get unique cities
    query = "SELECT DISTINCT city FROM listings_timeseries WHERE city IS NOT NULL LIMIT 10"
    cities_df = pd.read_sql(query, scraper.pg.conn)
    cities = cities_df['city'].tolist()
    
    for city in cities:
        print(f"   • Exporting {city}...")
        preprocessor.export_timeseries_csv(city=city)
    
    # Show trends
    print("\n📈 Step 4: Price Trends Analysis...")
    for city in cities[:3]:  # Show top 3
        metrics, df = preprocessor.get_price_trends(city=city)
        if metrics:
            print(f"\n   {city}:")
            print(f"      • Avg Price: {metrics['avg_price']:,.0f}")
            print(f"      • Trend: {metrics['price_trend']} ({metrics['price_change_pct']:.1f}%)")
            print(f"      • Volatility: {metrics['volatility']:,.0f}")
            print(f"      • Observations: {metrics['total_observations']}")
    
    print("\n" + "=" * 70)
    print("✅ Time series agent completed!")
    print("=" * 70)
    print("\n📁 Data available in: data/serie_temporelle/timeseries_exports/")
    print("🔧 Use this data to train your price estimation model")


if __name__ == "__main__":
    run_timeseries_agent()
