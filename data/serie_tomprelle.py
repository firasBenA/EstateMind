"""
Extract all scraped real estate data to CSV
============================================
Exports all listings from PostgreSQL to CSV file
"""

import pandas as pd
from datetime import datetime
import sys
from pathlib import Path

# Add parent directory to path to import postgres_client
sys.path.insert(0, str(Path(__file__).parent.parent))

from postgres_client import PostgresClient


def export_all_listings():
    """Export all listings to CSV"""
    print("🔄 Connecting to PostgreSQL...")
    
    try:
        pg = PostgresClient()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return
    
    print("📊 Querying all listings...")
    
    try:
        # Query all listings
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
                poi
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
        export_file = export_dir / f"all_listings_{timestamp}.csv"
        
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
        
        # Show price stats
        if df['price'].notna().sum() > 0:
            print(f"\n💰 Price statistics:")
            print(f"   • Min: {df['price'].min():,.0f}")
            print(f"   • Max: {df['price'].max():,.0f}")
            print(f"   • Mean: {df['price'].mean():,.0f}")
            print(f"   • Median: {df['price'].median():,.0f}")
        
        return export_file
        
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return None


def export_by_source():
    """Export listings by source to separate files"""
    print("\n🔄 Exporting by source...")
    
    try:
        pg = PostgresClient()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return
    
    try:
        query = "SELECT DISTINCT source_name FROM listings ORDER BY source_name"
        sources_df = pd.read_sql(query, pg.conn)
        sources = sources_df['source_name'].tolist()
        
        export_dir = Path(__file__).parent / "exports"
        export_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for source in sources:
            query = f"""
                SELECT * FROM listings 
                WHERE source_name = '{source}'
                ORDER BY scraped_at DESC
            """
            df = pd.read_sql(query, pg.conn)
            file_path = export_dir / f"{source}_listings_{timestamp}.csv"
            df.to_csv(file_path, index=False, encoding='utf-8')
            print(f"   ✅ {source}: {len(df):,} records → {file_path.name}")
        
        pg.conn.close()
        
    except Exception as e:
        print(f"❌ Failed: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("🏠 ESTATEMIND - DATA EXTRACTION TOOL")
    print("=" * 60)
    print()
    
    # Export all listings combined
    export_all_listings()
    
    # Optionally export by source
    print("\n" + "=" * 60)
    export_by_source()
    
    print("\n✅ Done!")
