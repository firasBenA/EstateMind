"""
Agent Monitoring Dashboard
===========================
Real-time monitoring and control of the intelligent scraping agent

Features:
- View live metrics
- See recent sessions
- Export data
- Control agent
- View logs
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from intelligent_scraping_agent import IntelligentScrapingAgent
import os


class AgentDashboard:
    """Dashboard for monitoring agent performance"""
    
    def __init__(self, db_path="agent_data.db"):
        """Initialize dashboard"""
        self.db_path = db_path
        self.agent = IntelligentScrapingAgent(db_path)
    
    def clear_screen(self):
        """Clear console screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def show_header(self):
        """Show dashboard header"""
        print("=" * 80)
        print(" " * 20 + "🤖 AGENT MONITORING DASHBOARD")
        print("=" * 80)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Database: {self.db_path}")
        print("-" * 80)
    
    def show_metrics(self):
        """Display current agent metrics"""
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
        """Show recent scraping sessions"""
        print("\n\n📋 RECENT SCRAPING SESSIONS")
        print("-" * 80)
        
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f'''
            SELECT 
                datetime(start_time) as start,
                datetime(end_time) as end,
                pages_scraped,
                listings_found,
                errors_count,
                strategy,
                CASE WHEN success = 1 THEN 'SUCCESS' ELSE 'FAILED' END as status
            FROM scraping_sessions
            ORDER BY start_time DESC
            LIMIT {limit}
        ''', conn)
        conn.close()
        
        if len(df) > 0:
            print(df.to_string(index=False))
        else:
            print("No sessions yet")
    
    def show_database_stats(self):
        """Show database statistics"""
        print("\n\n💾 DATABASE STATISTICS")
        print("-" * 80)
        
        conn = sqlite3.connect(self.db_path)
        
        # Total listings
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM listings")
        total = cursor.fetchone()[0]
        print(f"Total Listings in DB:    {total:,}")
        
        # Listings by region
        df_regions = pd.read_sql_query('''
            SELECT region, COUNT(*) as count
            FROM listings
            GROUP BY region
            ORDER BY count DESC
            LIMIT 10
        ''', conn)
        
        print(f"\nTop 10 Regions:")
        for idx, row in df_regions.iterrows():
            print(f"  {row['region']:20s} {row['count']:5d}")
        
        # Price statistics
        cursor.execute('''
            SELECT 
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM listings
            WHERE price IS NOT NULL
        ''')
        
        prices = cursor.fetchone()
        if prices and prices[0]:
            print(f"\nPrice Statistics:")
            print(f"  Average: {prices[0]:,.0f} TND")
            print(f"  Min:     {prices[1]:,.0f} TND")
            print(f"  Max:     {prices[2]:,.0f} TND")
        
        conn.close()
    
    def show_data_quality(self):
        """Show data quality metrics"""
        print("\n\n✅ DATA QUALITY")
        print("-" * 80)
        
        conn = sqlite3.connect(self.db_path)
        
        cursor = conn.cursor()
        
        # Completeness
        cursor.execute('''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) as with_price,
                SUM(CASE WHEN url IS NOT NULL THEN 1 ELSE 0 END) as with_url
            FROM listings
        ''')
        
        stats = cursor.fetchone()
        if stats and stats[0] > 0:
            total = stats[0]
            print(f"Completeness:")
            print(f"  With Price: {stats[1]/total*100:.1f}% ({stats[1]}/{total})")
            print(f"  With URL:   {stats[2]/total*100:.1f}% ({stats[2]}/{total})")
        
        # Average quality score
        cursor.execute('SELECT AVG(data_quality_score) FROM listings')
        avg_quality = cursor.fetchone()[0]
        
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
        
        conn.close()
    
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
            print(f"\n✅ Exported to: {filename}")
            input("\nPress Enter to continue...")
            
        elif choice == "2":
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query('''
                SELECT * FROM listings
                WHERE scraped_at >= datetime('now', '-7 days')
                ORDER BY scraped_at DESC
            ''', conn)
            conn.close()
            
            filename = f"export_7days_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\n✅ Exported {len(df)} listings to: {filename}")
            input("\nPress Enter to continue...")
            
        elif choice == "3":
            region = input("Enter region name: ").strip()
            
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query('''
                SELECT * FROM listings
                WHERE region LIKE ?
                ORDER BY scraped_at DESC
            ''', conn, params=(f'%{region}%',))
            conn.close()
            
            if len(df) > 0:
                filename = f"export_{region}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"\n✅ Exported {len(df)} listings to: {filename}")
            else:
                print(f"\n❌ No listings found for region: {region}")
            
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
            from intelligent_scraping_agent import ScrapingStrategy, ScrapingTask
            
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


def main():
    """Main dashboard execution"""
    dashboard = AgentDashboard()
    dashboard.show_main_menu()


if __name__ == "__main__":
    main()
