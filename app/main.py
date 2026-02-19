from scraping.storage import CSVStore
from scraping.postgres_store import PostgresStore
from scraping.dual_store import DualStore
from scraping.agent import ScrapeAgent
from scraping.adapters.mubawab import MubawabAdapter
from scraping.models import SearchParams

DB_URL = "postgresql://postgres:postgres@localhost:5432/realestate"

adapters = [MubawabAdapter()]
store = DualStore(PostgresStore(DB_URL), CSVStore(), keep_seen_file=True)

agent = ScrapeAgent(adapters, max_concurrency=3, store=store)

params_list = [
    SearchParams(transaction="rent", max_pages=10),
    SearchParams(transaction="sale", max_pages=10),
]

# puis: await agent.run_once(params_list)



# from fastapi import FastAPI
# from apscheduler.schedulers.asyncio import AsyncIOScheduler
# from datetime import date
# from pathlib import Path
# import pandas as pd

# from scraping.agent import ScrapeAgent
# from scraping.models import SearchParams

# # Import your adapters
# from scraping.adapters.mubawab import MubawabAdapter
# # from scraping.adapters.tayara import TayaraAdapter
# # from scraping.adapters.tunisie_annonce import TunisieAnnonceAdapter
# # from scraping.adapters.aqari import AqariAdapter

# app = FastAPI()

# @app.get("/debug/mubawab_agent")
# async def debug_mubawab_agent():
#     from scraping.adapters.mubawab import MubawabAdapter
#     from scraping.models import SearchParams

#     a = MubawabAdapter()
#     params = SearchParams(transaction="sale", max_pages=1)
#     urls = await a.search_urls(params)
#     first = None
#     if urls:
#         first_listing = await a.parse_listing(urls[0], params)
#         first = first_listing.model_dump()
#     await a.close()
#     return {"urls": len(urls), "first": first}

# @app.post("/scrape/run")
# async def run_now():
#     adapters = [MubawabAdapter()]
#     agent = ScrapeAgent(adapters, max_concurrency=3)
#     listings = await agent.run_once(build_params())
#     return {"status": "ok", "collected": len(listings)}

# def build_params():
#     # both rent & sale
#     params_list = [
#         SearchParams(transaction="rent", max_pages=10),
#         SearchParams(transaction="sale", max_pages=10),
#     ]
#     return params_list

# async def run_daily_scrape():
#     adapters = [
#         MubawabAdapter(),
#         # TayaraAdapter(),
#         # TunisieAnnonceAdapter(),
#         # AqariAdapter(),
#     ]
#     agent = ScrapeAgent(adapters, max_concurrency=3)
#     await agent.run_once(build_params())

# @app.on_event("startup")
# async def startup_event():
#     # Daily scheduler (note: avoid multiple workers running same job)
#     scheduler = AsyncIOScheduler()
#     scheduler.add_job(run_daily_scrape, "cron", hour=2, minute=0)
#     scheduler.start()

# # @app.post("/scrape/run")
# # async def run_now():
# #     await run_daily_scrape()
# #     return {"status": "ok", "date": date.today().isoformat()}

# @app.get("/scrape/latest")
# def latest_csv():
#     today = date.today().isoformat()
#     path = Path("data") / f"listings_{today}.csv"
#     return {"file": str(path), "exists": path.exists()}

# @app.get("/listings")
# def get_listings():
#     today = date.today().isoformat()
#     path = Path("data") / f"listings_{today}.csv"
#     if not path.exists():
#         return []
#     df = pd.read_csv(path)
#     return df.to_dict(orient="records")

# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(main())


