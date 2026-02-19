import asyncio
from scraping.adapters.mubawab import MubawabAdapter
from scraping.agent import ScrapeAgent
from scraping.models import SearchParams
from scraping.storage import CSVStore
from scraping.postgres_store import PostgresStore
from scraping.dual_store import DualStore
from scraping.image_pipeline import download_images_for_listings

DB_URL = "postgresql://postgres:postgres@localhost:5432/estatemind"  # adjust if needed

async def main():
    adapters = [MubawabAdapter()]
    store = DualStore(PostgresStore(DB_URL), CSVStore(), keep_seen_file=True)########True
    agent = ScrapeAgent(adapters, max_concurrency=3, store=store)

    # ✅ scrape both rent & sale, and ALL property types to multiply dataset
    params_list = []
    for txn in ["sale", "rent"]:
        for pt in ["apartment", "house", "villa", "land"]:
            params_list.append(
                SearchParams(
                    transaction=txn,
                    property_type=pt,
                    max_pages=500,   # increase if you want more
                )
            )

    listings = await agent.run_once(params_list)




    for listing in listings[:3]:
        print(listing.source_listing_id, "lat/lon:", listing.lat, listing.lon)
        print("images:", len(listing.image_urls))
        print("sample images:", listing.image_urls[:2])



    count = await download_images_for_listings(DB_URL, listings)
    print("downloaded images rows:", count)

    print("collected:", len(listings))
    if listings:
        print("first:", listings[0].model_dump())

    for a in adapters:
        await a.close()

asyncio.run(main())
