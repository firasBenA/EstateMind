import asyncio
import hashlib
from pathlib import Path
from typing import Iterable, Optional
import re

import httpx
from psycopg import connect
from psycopg.rows import dict_row

from scraping.models import Listing

IMAGES_DIR = Path("images")

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def guess_ext(content_type: str, url: str) -> str:
    ct = (content_type or "").lower()
    if "avif" in ct or url.lower().endswith(".avif"):
        return "avif"
    if "webp" in ct or url.lower().endswith(".webp"):
        return "webp"
    if "png" in ct or url.lower().endswith(".png"):
        return "png"
    if "jpeg" in ct or "jpg" in ct or url.lower().endswith((".jpg", ".jpeg")):
        return "jpg"
    return "jpg"

def is_listing_image(url: str) -> bool:
    # Mubawab listing images contain /ad/
    return "/ad/" in url

async def download_images_for_listings(
    db_url: str,
    listings: Iterable[Listing],
    max_concurrency: int = 8,
    timeout_s: float = 30.0,
    retries: int = 2,
):
    IMAGES_DIR.mkdir(exist_ok=True)

    listings = list(listings)
    if not listings:
        return 0

    # Build candidate download jobs
    jobs = []
    for listing in listings:
        if not listing.image_urls:
            continue

        base_dir = IMAGES_DIR / listing.source / listing.source_listing_id
        base_dir.mkdir(parents=True, exist_ok=True)

        # filter & dedupe
        seen_u = set()
        filtered = []
        for u in listing.image_urls:
            if not u or u in seen_u:
                continue
            seen_u.add(u)
            if is_listing_image(u):
                filtered.append(u)

        for pos, url in enumerate(filtered):
            jobs.append((listing, pos, url, base_dir))

    if not jobs:
        return 0

    # Skip if already in DB (storage_key exists)
    # We can't know sha before download, so we'll skip using (source,id,original_url) matches
    existing_original_urls = set()
    with connect(db_url, row_factory=dict_row) as conn:
        existing_original_urls = set()
        pairs = [(l.source, l.source_listing_id) for l in listings]
        if pairs:
            values_sql = ",".join(["(%s,%s)"] * len(pairs))
            flat = [x for p in pairs for x in p]
            sql = f"""
            WITH k(source, source_listing_id) AS (VALUES {values_sql})
            SELECT li.original_url
            FROM listing_images li
            JOIN k ON k.source = li.source AND k.source_listing_id = li.source_listing_id;
            """
            with connect(db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, flat)
                    for (u,) in cur.fetchall():
                        existing_original_urls.add(u)


    sem = asyncio.Semaphore(max_concurrency)
    rows_to_insert = []

    async def fetch_one(client: httpx.AsyncClient, listing: Listing, pos: int, url: str, base_dir: Path):
        if url in existing_original_urls:
            return  # already stored -> skip

        async with sem:
            for attempt in range(retries + 1):
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    content = r.content
                    digest = sha256_bytes(content)
                    ext = guess_ext(r.headers.get("Content-Type", ""), url)

                    filename = f"{pos:02d}_{digest}.{ext}"
                    path = base_dir / filename

                    if not path.exists():
                        path.write_bytes(content)

                    storage_key = f"{listing.source}/{listing.source_listing_id}/{filename}"

                    rows_to_insert.append({
                        "source": listing.source,
                        "source_listing_id": listing.source_listing_id,
                        "original_url": url,
                        "storage_key": storage_key,
                        "local_path": str(path),
                        "public_url": None,
                        "content_type": r.headers.get("Content-Type"),
                        "sha256": digest,
                        "position": pos,
                    })
                    return
                except Exception:
                    if attempt >= retries:
                        return
                    await asyncio.sleep(0.6 * (attempt + 1))

    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        await asyncio.gather(*(fetch_one(client, l, pos, url, base_dir) for (l, pos, url, base_dir) in jobs))

    if not rows_to_insert:
        return 0

    sql = """
    INSERT INTO listing_images (
      source, source_listing_id,
      original_url, storage_key, local_path, public_url,
      content_type, sha256, position
    )
    VALUES (
      %(source)s, %(source_listing_id)s,
      %(original_url)s, %(storage_key)s, %(local_path)s, %(public_url)s,
      %(content_type)s, %(sha256)s, %(position)s
    )
    ON CONFLICT (storage_key) DO NOTHING;
    """

    with connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows_to_insert)
        conn.commit()

    return len(rows_to_insert)
