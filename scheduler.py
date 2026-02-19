
import time
import schedule

from config.logging_config import log
from core.pipeline import ScrapingPipeline
from scrapers import (
    
    AqariScraper,
    ZitounaImmoScraper,
    RemaxScraper,
    Century21Scraper,
    DarcomScraper,
    VerdarScraper,
    NewKeyScraper,
    TPSImmobiliereScraper,
    EdfScraper,
  
    CabinetImmoScraper,
    AzurImmobiliereScraper,
    BnbScraper,
    MouinImmobilierScraper,
    MenziliScraper,
    CactusImmobilierScraper,
)


def run_job():
    log.info("Starting scheduled scraping job...")
    scrapers = [
        # TayaraScraper(),
        AqariScraper(),
        ZitounaImmoScraper(),
        RemaxScraper(),
        Century21Scraper(),
        DarcomScraper(),
        VerdarScraper(),
        NewKeyScraper(),
        TPSImmobiliereScraper(),
        EdfScraper(),
        CabinetImmoScraper(),
        AzurImmobiliereScraper(),
        BnbScraper(),
        MouinImmobilierScraper(),
        MenziliScraper(),
        CactusImmobilierScraper(),
    ]
    pipeline = ScrapingPipeline(scrapers)
    pipeline.run()
    log.info("Scheduled job finished.")


def start_scheduler():
    log.info("Scheduler started. Running every 24 hours.")
    schedule.every(24).hours.do(run_job)
    run_job()
    while True:
        schedule.run_pending()
        time.sleep(1)
