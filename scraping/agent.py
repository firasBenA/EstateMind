import asyncio
import logging
from scraping.models import SearchParams, Listing
from scraping.storage import CSVStore
from scraping.utils import canonical_url

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ScrapeAgent:
    def __init__(self, adapters, max_concurrency: int = 3, store=None):
        self.adapters = adapters
        self.max_concurrency = max_concurrency  # ✅ FIX
        self.sem = asyncio.Semaphore(max_concurrency)
        self.store = store or CSVStore()        # ✅ store injectable

    async def run_once(self, params_list):
        seen = self.store.load_seen()
        logger.info("Seen loaded: %d keys", len(seen))
        collected = []
        errors = 0

        async def worker(adapter, params, q: asyncio.Queue):
            nonlocal errors
            while True:
                url = await q.get()
                if url is None:
                    q.task_done()
                    return
                try:
                    url2 = canonical_url(url)

                    listing = await self._parse_one(adapter, url2, params)
                    if listing is None:
                        continue

                    key = f"{listing.source}:{listing.source_listing_id}"
                    if key in seen:
                        continue

                    seen.add(key)
                    collected.append(listing)

                except Exception as e:
                    errors += 1
                    logger.exception("[%s] worker parse failed: %s", adapter.source_name, e)
                finally:
                    q.task_done()

        for params in params_list:
            for adapter in self.adapters:
                urls = await adapter.search_urls(params)

                q = asyncio.Queue()
                for u in urls:
                    q.put_nowait(u)

                workers = [
                    asyncio.create_task(worker(adapter, params, q))
                    for _ in range(self.max_concurrency)
                ]

                for _ in workers:
                    q.put_nowait(None)

                await q.join()
                await asyncio.gather(*workers)

        # Persist seen + save results
        self.store.save_seen(seen)
        if collected:
            self.store.save(collected)

        logger.info("Collected=%d errors=%d", len(collected), errors)
        return collected

    async def _parse_one(self, adapter, url: str, params: SearchParams) -> Listing:
        async with self.sem:
            await adapter.polite_delay()
            return await adapter.parse_listing(url, params)
