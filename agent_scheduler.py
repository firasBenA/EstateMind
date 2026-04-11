"""
Autonomous Agent Scheduler
===========================
Continuously runs the intelligent scraping agent on a schedule

Features:
- Automatic scheduling (daily, hourly, etc.)
- Runs in background
- Monitors agent health
- Sends notifications
- Can run indefinitely
"""

import schedule
import time
import logging
from datetime import datetime
from intelligent_scraping_agent import IntelligentScrapingAgent, AgentState
import threading
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AgentScheduler:
    """
    Scheduler for autonomous agent execution
    
    Manages when and how the agent runs
    """
    
    def __init__(self):
        """Initialize scheduler"""
        self.agent = IntelligentScrapingAgent()
        self.running = True
        self.total_cycles = 0
        self.successful_cycles = 0
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
        logger.info("🕐 Scheduler initialized")
    
    def run_agent_cycle(self):
        """Run one agent cycle"""
        logger.info("\n" + "=" * 70)
        logger.info(f"🔄 SCHEDULED CYCLE #{self.total_cycles + 1}")
        logger.info(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
        
        try:
            success = self.agent.run_autonomous_cycle()
            
            self.total_cycles += 1
            if success:
                self.successful_cycles += 1
            
            # Report scheduler stats
            success_rate = (self.successful_cycles / self.total_cycles * 100) if self.total_cycles > 0 else 0
            logger.info(f"\n📈 Scheduler Stats:")
            logger.info(f"   Total Cycles: {self.total_cycles}")
            logger.info(f"   Successful: {self.successful_cycles}")
            logger.info(f"   Success Rate: {success_rate:.1f}%")
            
        except Exception as e:
            logger.error(f"❌ Error in agent cycle: {e}")
            self.total_cycles += 1
    
    def schedule_tasks(self):
        """Setup task schedule"""
        logger.info("\n📅 Setting up schedule...")
        
        # Daily scrape at 3 AM
        schedule.every().day.at("03:00").do(self.run_agent_cycle)
        logger.info("   ✅ Daily scrape: 3:00 AM")
        
        # Quick check every 6 hours
        schedule.every(6).hours.do(self.run_agent_cycle)
        logger.info("   ✅ Quick check: Every 6 hours")
        
        # Weekly full scrape on Sunday
        schedule.every().sunday.at("02:00").do(self.run_full_scrape)
        logger.info("   ✅ Full scrape: Sunday 2:00 AM")
        
        logger.info("\n✅ Schedule configured!")
    
    def run_full_scrape(self):
        """Run a full comprehensive scrape"""
        logger.info("\n🌟 FULL SCRAPE MODE")
        
        # Temporarily override agent settings for comprehensive scrape
        from intelligent_scraping_agent import ScrapingTask, ScrapingStrategy
        
        task = ScrapingTask(
            max_pages=50,  # Scrape more pages
            delay=2,
            strategy=ScrapingStrategy.AGGRESSIVE,
            priority=3
        )
        
        self.agent.execute_task(task)
        self.agent.export_data(f"weekly_export_{datetime.now().strftime('%Y%m%d')}.csv")
    
    def run_once_now(self):
        """Run agent immediately once"""
        logger.info("\n🚀 Running agent once (immediate execution)...")
        self.run_agent_cycle()
    
    def run_continuous(self):
        """Run scheduler continuously"""
        logger.info("\n🔁 Starting continuous mode...")
        logger.info("   Press Ctrl+C to stop")
        
        self.schedule_tasks()
        
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown"""
        logger.info("\n⚠️  Shutdown signal received")
        logger.info("   Finishing current task...")
        
        self.running = False
        
        # Save final metrics
        self.agent._save_metrics()
        self.agent.report_metrics()
        
        logger.info("\n👋 Scheduler stopped gracefully")
        sys.exit(0)


def main():
    """Main execution"""
    print("=" * 70)
    print("🤖 AUTONOMOUS AGENT SCHEDULER")
    print("=" * 70)
    print()
    print("Options:")
    print("1. Run once now")
    print("2. Start continuous scheduled mode")
    print("3. Exit")
    print()
    
    choice = input("Choose (1-3): ").strip()
    
    scheduler = AgentScheduler()
    
    if choice == "1":
        scheduler.run_once_now()
        scheduler.agent.export_data()
        print("\n✅ Single run complete!")
        
    elif choice == "2":
        # Run once immediately, then start schedule
        scheduler.run_once_now()
        scheduler.run_continuous()
        
    else:
        print("👋 Goodbye!")


if __name__ == "__main__":
    main()
