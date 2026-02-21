"""
Agent Monitoring Dashboard - PostgreSQL Only
=============================================
Real-time monitoring and control of the intelligent scraping agent

Features:
- View live metrics from PostgreSQL
- See recent sessions
- Export data
- Control agent
- View logs

Uses your existing PostgresClient
"""

import pandas as pd
from datetime import datetime
import os

from postgres_client import PostgresClient
from intelligent_scraping_agent import IntelligentScrapingAgent, ScrapingStrategy


class AgentDashboard:
    """Dashboard for monitoring agent performance - PostgreSQL backend"""
    
    def __init__(self):
        """Initialize dashboard with PostgreSQL"""
        self.pg = PostgresClient()
        self.agent = IntelligentScrapingAgent()
    
    def clear_screen(self):
        """Clear console screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def show_header(self):
        """Show dashboard header"""
        print("=" * 80)
        print(" " * 20 + "🤖 AGENT MONITORING DASHBOARD")
        print("=" * 80)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Database: estatemind (PostgreSQL)")
        print("-" * 80)
    
    def show_metrics(self):
        """Display current agent metrics from PostgreSQL"""
        print("\n📊 AGENT METRICS")
        print("-" * 80)
        
        metrics = self.agent.metrics
        
        print(f"Total Listings Scraped:  {metrics.total_listings_scraped:,}")
        print(f"Total Pages Scraped:     {metrics.total_pages_scraped:,}")
        print(f"Successful Scrapes:      {metrics.successful_scrapes}")
        print(f"Failed Scrapes:          {metrics.failed_scrapes}")
        
        if metrics.successful_scrapes + metrics.failed_scrapes > 0:
            error_rate = metrics.failed_scrapes / (metrics.successful_scrapes + metrics.failed_scrapes) * 100
            print(f"Error Rate:              {error_rate:.1f}%")
        
        print(f"Data Quality Score:      {metrics.data_quality_score:.1f}%")
        print(f"Self-Heals Performed:    {metrics.self_heals_performed}")
        print(f"Average Scrape Time:     {metrics.average_scrape_time:.1f}s")
        print(f"Last Scrape:             {metrics.last_scrape_time or 'Never'}")
    
    def show_recent_sessions(self, limit=5):
        """Show recent scraping sessions from PostgreSQL"""
        print("\n\n📋 RECENT SCRAPING SESSIONS")
        print("-" * 80)
        
        try:
            with self.pg.conn.cursor() as cur:
                cur.execute(f"""
                    SELECT 
                        TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS') as start,
                        TO_CHAR(end_time, 'YYYY-MM-DD HH24:MI:SS') as end,
                        pages_scraped,
                        listings_found,
                        errors_count,
                        strategy,
                        CASE WHEN success THEN 'SUCCESS' ELSE 'FAILED' END as status
                    FROM scraping_sessions
                    ORDER BY start_time DESC
                    LIMIT {limit}
                """)
                
                rows = cur.fetchall()
                
                if rows:
                    # Create DataFrame for nice display
                    df = pd.DataFrame(rows, columns=['start', 'end', 'pages', 'listings', 'errors', 'strategy', 'status'])
                    print(df.to_string(index=False))
                else:
                    print("No sessions yet")
                    
        except Exception as e:
            print(f"Error loading sessions: {e}")
    
    def show_database_stats(self):
        """Show database statistics from PostgreSQL"""
        print("\n\n💾 DATABASE STATISTICS")
        print("-" * 80)
        
        try:
            with self.pg.conn.cursor() as cur:
                # Total listings
                cur.execute("SELECT COUNT(*) FROM listings WHERE source_name = 'tunisieannonce'")
                total = cur.fetchone()[0]
                print(f"Total Listings in DB:    {total:,}")
                
                if total > 0:
                    # Listings by region
                    cur.execute("""
                        SELECT region, COUNT(*) as count
                        FROM listings
                        WHERE source_name = 'tunisieannonce'
                        GROUP BY region
                        ORDER BY count DESC
                        LIMIT 10
                    """)
                    
                    print(f"\nTop 10 Regions:")
                    for region, count in cur.fetchall():
                        print(f"  {region:20s} {count:5d}")
                    
                    # Price statistics
                    cur.execute("""
                        SELECT 
                            AVG(price) as avg_price,
                            MIN(price) as min_price,
                            MAX(price) as max_price
                        FROM listings
                        WHERE source_name = 'tunisieannonce'
                          AND price IS NOT NULL
                    """)
                    
                    stats = cur.fetchone()
                    if stats and stats[0]:
                        print(f"\nPrice Statistics:")
                        print(f"  Average: {stats[0]:,.0f} {stats[0] and 'TND' or ''}")
                        print(f"  Min:     {stats[1]:,.0f} TND")
                        print(f"  Max:     {stats[2]:,.0f} TND")
                    
                    # Property type distribution
                    cur.execute("""
                        SELECT type, COUNT(*) as count
                        FROM listings
                        WHERE source_name = 'tunisieannonce'
                        GROUP BY type
                        ORDER BY count DESC
                        LIMIT 5
                    """)
                    
                    print(f"\nTop 5 Property Types:")
                    for prop_type, count in cur.fetchall():
                        print(f"  {prop_type:20s} {count:5d}")
                    
        except Exception as e:
            print(f"Error loading statistics: {e}")
    
    def show_data_quality(self):
        """Show data quality metrics from PostgreSQL"""
        print("\n\n✅ DATA QUALITY")
        print("-" * 80)
        
        try:
            with self.pg.conn.cursor() as cur:
                # Completeness
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(price) as with_price,
                        COUNT(url) as with_url,
                        COUNT(description) as with_description
                    FROM listings
                    WHERE source_name = 'tunisieannonce'
                """)
                
                stats = cur.fetchone()
                if stats and stats[0] > 0:
                    total = stats[0]
                    print(f"Completeness:")
                    print(f"  With Price:       {stats[1]/total*100:.1f}% ({stats[1]}/{total})")
                    print(f"  With URL:         {stats[2]/total*100:.1f}% ({stats[2]}/{total})")
                    print(f"  With Description: {stats[3]/total*100:.1f}% ({stats[3]}/{total})")
                
                # Average quality score from features
                cur.execute("""
                    SELECT AVG((features->>'data_quality_score')::float)
                    FROM listings
                    WHERE source_name = 'tunisieannonce'
                      AND features ? 'data_quality_score'
                """)
                
                avg_quality = cur.fetchone()[0]
                
                if avg_quality:
                    print(f"\nAverage Quality Score: {avg_quality:.1f}%")
                    
                    if avg_quality >= 90:
                        print("  Status: Excellent ⭐⭐⭐")
                    elif avg_quality >= 80:
                        print("  Status: Good ⭐⭐")
                    elif avg_quality >= 70:
                        print("  Status: Fair ⭐")
                    else:
                        print("  Status: Needs Improvement ⚠️")
                
                # Recent scraping activity
                cur.execute("""
                    SELECT 
                        DATE(scraped_at) as date,
                        COUNT(*) as listings
                    FROM listings
                    WHERE source_name = 'tunisieannonce'
                      AND scraped_at >= CURRENT_DATE - INTERVAL '7 days'
                    GROUP BY DATE(scraped_at)
                    ORDER BY date DESC
                """)
                
                print(f"\nRecent Activity (Last 7 days):")
                for date, count in cur.fetchall():
                    print(f"  {date}: {count} listings")
                    
        except Exception as e:
            print(f"Error loading quality metrics: {e}")
    
    def export_data_menu(self):
        """Export data options"""
        print("\n\n📤 EXPORT DATA")
        print("-" * 80)
        print("1. Export all data to CSV")
        print("2. Export last 7 days")
        print("3. Export specific region")
        print("4. Back to main menu")
        
        choice = input("\nChoice (1-4): ").strip()
        
        if choice == "1":
            filename = self.agent.export_data()
            if filename:
                print(f"\n✅ Exported to: {filename}")
            else:
                print(f"\n❌ Export failed")
            input("\nPress Enter to continue...")
            
        elif choice == "2":
            try:
                with self.pg.conn.cursor() as cur:
                    cur.execute("""
                        SELECT * FROM listings
                        WHERE source_name = 'tunisieannonce'
                          AND scraped_at >= CURRENT_DATE - INTERVAL '7 days'
                        ORDER BY scraped_at DESC
                    """)
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    df = pd.DataFrame(rows, columns=columns)
                    
                    filename = f"export_7days_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    df.to_csv(filename, index=False, encoding='utf-8-sig')
                    print(f"\n✅ Exported {len(df)} listings to: {filename}")
                    
            except Exception as e:
                print(f"\n❌ Export failed: {e}")
            
            input("\nPress Enter to continue...")
            
        elif choice == "3":
            region = input("Enter region name: ").strip()
            
            try:
                with self.pg.conn.cursor() as cur:
                    cur.execute("""
                        SELECT * FROM listings
                        WHERE source_name = 'tunisieannonce'
                          AND region ILIKE %s
                        ORDER BY scraped_at DESC
                    """, (f'%{region}%',))
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    if rows:
                        df = pd.DataFrame(rows, columns=columns)
                        filename = f"export_{region}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                        df.to_csv(filename, index=False, encoding='utf-8-sig')
                        print(f"\n✅ Exported {len(df)} listings to: {filename}")
                    else:
                        print(f"\n❌ No listings found for region: {region}")
                        
            except Exception as e:
                print(f"\n❌ Export failed: {e}")
            
            input("\nPress Enter to continue...")
    
    def control_agent_menu(self):
        """Agent control options"""
        print("\n\n🎮 AGENT CONTROL")
        print("-" * 80)
        print("1. Run agent now (balanced)")
        print("2. Run agent now (aggressive)")
        print("3. Run agent now (conservative)")
        print("4. View agent logs")
        print("5. Back to main menu")
        
        choice = input("\nChoice (1-5): ").strip()
        
        if choice in ["1", "2", "3"]:
            strategy_map = {
                "1": ScrapingStrategy.BALANCED,
                "2": ScrapingStrategy.AGGRESSIVE,
                "3": ScrapingStrategy.CONSERVATIVE
            }
            
            strategy = strategy_map[choice]
            task = self.agent.create_task_from_strategy(strategy)
            
            print(f"\n🚀 Running agent with {strategy.value} strategy...")
            print(f"   Pages: {task.max_pages}, Delay: {task.delay}s\n")
            
            success = self.agent.execute_task(task)
            
            if success:
                print("\n✅ Task completed successfully!")
            else:
                print("\n⚠️  Task completed with issues")
            
            input("\nPress Enter to continue...")
            
        elif choice == "4":
            print("\n📄 Last 20 lines of agent.log:")
            print("-" * 80)
            try:
                with open('agent.log', 'r') as f:
                    lines = f.readlines()
                    for line in lines[-20:]:
                        print(line.rstrip())
            except FileNotFoundError:
                print("No log file found")
            
            input("\nPress Enter to continue...")
    
    def show_main_menu(self):
        """Show main dashboard menu"""
        while True:
            self.clear_screen()
            self.show_header()
            self.show_metrics()
            self.show_recent_sessions()
            
            print("\n\n" + "=" * 80)
            print("MENU")
            print("=" * 80)
            print("1. View database statistics")
            print("2. View data quality")
            print("3. Export data")
            print("4. Control agent")
            print("5. Refresh dashboard")
            print("6. Exit")
            
            choice = input("\nChoice (1-6): ").strip()
            
            if choice == "1":
                self.clear_screen()
                self.show_header()
                self.show_database_stats()
                input("\n\nPress Enter to continue...")
                
            elif choice == "2":
                self.clear_screen()
                self.show_header()
                self.show_data_quality()
                input("\n\nPress Enter to continue...")
                
            elif choice == "3":
                self.export_data_menu()
                
            elif choice == "4":
                self.control_agent_menu()
                
            elif choice == "5":
                continue
                
            elif choice == "6":
                print("\n👋 Goodbye!")
                break
    
    def close(self):
        """Close connections"""
        try:
            self.pg.close()
            self.agent.close()
        except:
            pass


def main():
    """Main dashboard execution"""
    print("=" * 80)
    print("🤖 AGENT DASHBOARD - POSTGRESQL")
    print("=" * 80)
    print()
    
    try:
        dashboard = AgentDashboard()
        dashboard.show_main_menu()
        dashboard.close()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()