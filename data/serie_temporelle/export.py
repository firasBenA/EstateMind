"""
Time Series Data Export Agent
==============================
Exports all scraped real estate data with dates for time series analysis
"""

import pandas as pd
from datetime import datetime
import sys
from pathlib import Path

# Add parent directory to path to import postgres_client
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from postgres_client import PostgresClient


def export_all_timeseries_data():
    """Export all listings with date information for time series analysis"""
    print("🔄 Connecting to PostgreSQL...")

    try:
        pg = PostgresClient()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    print("📊 Querying all listings with dates...")

    try:
        # Query all listings with date information
        query = """
            SELECT
                id,
                property_id,
                source_name,
                url,
                type,
                title,
                description,
                price,
                surface,
                rooms,
                region,
                zone,
                city,
                municipalite,
                latitude,
                longitude,
                pdf_link,
                images,
                features,
                scraped_at,
                last_update,
                transaction_type,
                currency,
                raw_data_path,
                poi,
                -- Add date components for time series analysis
                EXTRACT(YEAR FROM scraped_at) as year,
                EXTRACT(MONTH FROM scraped_at) as month,
                EXTRACT(DAY FROM scraped_at) as day,
                DATE(scraped_at) as listing_date
            FROM listings
            ORDER BY scraped_at DESC
        """

        df = pd.read_sql(query, pg.conn)
        pg.conn.close()

        total_records = len(df)

        if total_records == 0:
            print("⚠️  No listings found in database")
            return

        # Create exports directory if it doesn't exist
        export_dir = Path(__file__).parent / "exports"
        export_dir.mkdir(exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_file = export_dir / f"timeseries_all_data_{timestamp}.csv"

        # Export to CSV
        df.to_csv(export_file, index=False, encoding='utf-8')

        print(f"✅ Export successful!")
        print(f"   📁 File: {export_file}")
        print(f"   📈 Records: {total_records:,}")
        print(f"   📊 Columns: {len(df.columns)}")

        # Show source breakdown
        print("\n📍 Listing by source:")
        source_counts = df['source_name'].value_counts()
        for source, count in source_counts.items():
            print(f"   • {source}: {count:,}")

        # Show date range
        if 'listing_date' in df.columns and not df['listing_date'].isna().all():
            min_date = df['listing_date'].min()
            max_date = df['listing_date'].max()
            print(f"\n📅 Date range: {min_date} to {max_date}")

        # Show price stats
        if df['price'].notna().sum() > 0:
            print(f"\n💰 Price statistics:")
            print(f"   • Min: {df['price'].min():,.0f}")
            print(f"   • Max: {df['price'].max():,.0f}")
            print(f"   • Mean: {df['price'].mean():,.0f}")
            print(f"   • Median: {df['price'].median():,.0f}")

        # Show city breakdown
        if 'city' in df.columns and not df['city'].isna().all():
            print(f"\n🏙️  Top cities:")
            city_counts = df['city'].value_counts().head(5)
            for city, count in city_counts.items():
                print(f"   • {city}: {count:,}")

        return export_file

    except Exception as e:
        print(f"❌ Export failed: {e}")
        return None


if __name__ == "__main__":
    print("=" * 60)
    print("📊 TIME SERIES DATA EXPORT AGENT")
    print("=" * 60)
    print()

    # Export all data with dates
    export_all_timeseries_data()

    print("\n✅ Done!")
    print("\n📁 Data available in: data/serie_temporelle/exports/")
    print("🔧 Ready for time series modeling and price estimation")
