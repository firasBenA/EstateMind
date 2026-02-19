from abc import ABC, abstractmethod
from typing import List
import random
import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from scraping.models import SearchParams, Listing

class BaseAdapter(ABC):
    source_name: str

    def __init__(self, timeout=30.0, user_agent: str = "Mozilla/5.0"):
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": user_agent},
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def polite_delay(self):
        await asyncio.sleep(random.uniform(0.8, 2.0))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def fetch(self, url: str) -> str:
        r = await self.client.get(url)
        r.raise_for_status()
        return r.text

    @abstractmethod
    async def search_urls(self, params: SearchParams) -> List[str]:
        ...

    @abstractmethod
    async def parse_listing(self, url: str, params: SearchParams) -> Listing:
        ...
