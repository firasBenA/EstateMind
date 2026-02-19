import argparse
import sys
from scheduler import start_scheduler, run_job
from config.logging_config import log

def main():
    parser = argparse.ArgumentParser(description="EstateMind Scraping Framework")
    parser.add_argument("action", choices=["run", "schedule"], help="Action to perform")
    
    args = parser.parse_args()
    
    if args.action == "run":
        log.info("Running manual scraping...")
        run_job()
    elif args.action == "schedule":
        start_scheduler()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopping...")
        sys.exit(0)
    except Exception as e:
        log.critical(f"Unhandled exception: {e}")
        sys.exit(1)
