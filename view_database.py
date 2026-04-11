"""
Quick Database Viewer
=====================
Simple script to view agent_data.db contents
"""

import sqlite3
import pandas as pd

def view_database(db_path="agent_data.db"):
    """View all tables and their contents"""
    
    print("=" * 80)
    print("DATABASE VIEWER - agent_data.db")
    print("=" * 80)
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Get list of tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print(f"\n📋 Found {len(tables)} tables:")
        for table in tables:
            print(f"   - {table[0]}")
        
        print("\n" + "=" * 80)
        
        # View each table
        for table in tables:
            table_name = table[0]
            
            print(f"\n📊 TABLE: {table_name}")
            print("-" * 80)
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"Total rows: {count}")
            
            if count > 0:
                # Show first 5 rows
                df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 5", conn)
                print(f"\nFirst 5 rows:")
                print(df.to_string(index=False))
                
                # Show column info
                print(f"\nColumns ({len(df.columns)}):")
                for col in df.columns:
                    print(f"   - {col}: {df[col].dtype}")
            else:
                print("   (Empty table)")
            
            print("\n" + "-" * 80)
        
        conn.close()
        
        print("\n✅ Database viewed successfully!")
        
    except FileNotFoundError:
        print(f"\n❌ Database file not found: {db_path}")
        print("   Run the agent first to create the database:")
        print("   python intelligent_scraping_agent.py")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")


def search_listings(keyword=None, region=None, max_price=None):
    """Search listings with filters"""
    
    print("\n" + "=" * 80)
    print("SEARCH LISTINGS")
    print("=" * 80)
    
    conn = sqlite3.connect("agent_data.db")
    
    # Build query
    query = "SELECT * FROM listings WHERE 1=1"
    params = []
    
    if keyword:
        query += " AND (description LIKE ? OR type LIKE ?)"
        params.extend([f'%{keyword}%', f'%{keyword}%'])
    
    if region:
        query += " AND region LIKE ?"
        params.append(f'%{region}%')
    
    if max_price:
        query += " AND price <= ?"
        params.append(max_price)
    
    query += " ORDER BY scraped_at DESC LIMIT 20"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    print(f"\nFound {len(df)} listings:")
    
    if len(df) > 0:
        # Show key columns
        cols = ['region', 'type', 'price', 'description', 'scraped_at']
        display_cols = [col for col in cols if col in df.columns]
        print(df[display_cols].to_string(index=False))
    else:
        print("No results found")


def show_statistics():
    """Show database statistics"""
    
    print("\n" + "=" * 80)
    print("DATABASE STATISTICS")
    print("=" * 80)
    
    conn = sqlite3.connect("agent_data.db")
    
    # Total listings
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM listings")
    total = cursor.fetchone()[0]
    print(f"\n📊 Total Listings: {total:,}")
    
    if total > 0:
        # By region
        print("\n📍 Listings by Region (Top 10):")
        df_region = pd.read_sql_query('''
            SELECT region, COUNT(*) as count
            FROM listings
            GROUP BY region
            ORDER BY count DESC
            LIMIT 10
        ''', conn)
        print(df_region.to_string(index=False))
        
        # Price stats
        print("\n💰 Price Statistics:")
        cursor.execute('''
            SELECT 
                COUNT(*) as with_price,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM listings
            WHERE price IS NOT NULL
        ''')
        stats = cursor.fetchone()
        print(f"   Listings with price: {stats[0]:,}")
        print(f"   Average: {stats[1]:,.0f} TND")
        print(f"   Min: {stats[2]:,.0f} TND")
        print(f"   Max: {stats[3]:,.0f} TND")
        
        # Recent activity
        print("\n🕐 Recent Activity:")
        df_recent = pd.read_sql_query('''
            SELECT 
                DATE(scraped_at) as date,
                COUNT(*) as listings
            FROM listings
            GROUP BY DATE(scraped_at)
            ORDER BY date DESC
            LIMIT 7
        ''', conn)
        print(df_recent.to_string(index=False))
    
    conn.close()


def export_to_excel():
    """Export all tables to Excel"""
    
    print("\n" + "=" * 80)
    print("EXPORT TO EXCEL")
    print("=" * 80)
    
    try:
        conn = sqlite3.connect("agent_data.db")
        
        # Create Excel writer
        filename = f"database_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Export each table
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                df.to_excel(writer, sheet_name=table_name[:31], index=False)  # Excel sheet name limit
                print(f"   ✅ Exported table: {table_name} ({len(df)} rows)")
        
        conn.close()
        
        print(f"\n✅ Exported to: {filename}")
        
    except ImportError:
        print("\n❌ openpyxl not installed. Install with:")
        print("   pip install openpyxl")
    except Exception as e:
        print(f"\n❌ Error: {e}")


def main():
    """Interactive menu"""
    
    while True:
        print("\n" + "=" * 80)
        print("DATABASE VIEWER MENU")
        print("=" * 80)
        print("1. View all tables")
        print("2. Show statistics")
        print("3. Search listings")
        print("4. Export to Excel")
        print("5. Export to CSV")
        print("6. Exit")
        
        choice = input("\nChoice (1-6): ").strip()
        
        if choice == "1":
            view_database()
            
        elif choice == "2":
            show_statistics()
            
        elif choice == "3":
            print("\nSearch filters (press Enter to skip):")
            keyword = input("Keyword (description/type): ").strip() or None
            region = input("Region: ").strip() or None
            max_price_str = input("Max price: ").strip()
            max_price = int(max_price_str) if max_price_str else None
            
            search_listings(keyword, region, max_price)
            
        elif choice == "4":
            export_to_excel()
            
        elif choice == "5":
            conn = sqlite3.connect("agent_data.db")
            df = pd.read_sql_query("SELECT * FROM listings", conn)
            conn.close()
            
            filename = f"listings_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\n✅ Exported {len(df)} listings to: {filename}")
            
        elif choice == "6":
            print("\n👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice")


if __name__ == "__main__":
    main()