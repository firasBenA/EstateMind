import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database.vector_db import VectorDBHandler, _clean_metadata
from scrapers.all_scrapers import build_all_scrapers


TECNOCASA_DETAIL_RE = re.compile(
    r"^https?://(www\\.)?tecnocasa\\.tn/(vendre|louer)/.+?/(\\d+)\\.html(?:$|[?#])",
    re.IGNORECASE,
)

SOURCE_URL_PATTERNS: Dict[str, re.Pattern] = {
    "affare": re.compile(r"https?://(www\\.)?affare\\.tn/.*/annonce/", re.IGNORECASE),
    "century21": re.compile(r"https?://(www\\.)?century21\\.tn/.*/property/", re.IGNORECASE),
    "darcom": re.compile(r"https?://(www\\.)?darcomtunisia\\.com/.*/bien/details/", re.IGNORECASE),
    "mubawab": re.compile(r"https?://(www\\.)?mubawab\\.tn/.*/fr/pa/\\d+", re.IGNORECASE),
    "newkey": re.compile(r"https?://(www\\.)?newkey\\.com\\.tn/.*/bien/details/", re.IGNORECASE),
    "tecnocasa": TECNOCASA_DETAIL_RE,
    "tunisieannonce": re.compile(r"https?://(www\\.)?tunisie-annonce\\.com/Details_Annonces_Immobilier\\.asp\\?cod_ann=\\d+", re.IGNORECASE),
    "verdar": re.compile(r"https?://(www\\.)?verdar\\.tn/.*/bien/details/", re.IGNORECASE),
    "zitouna_immo": re.compile(r"https?://(www\\.)?zitounaimmo\\.com/.*/bien/details/", re.IGNORECASE),
}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _get_source_and_id(vector_id: str) -> Tuple[Optional[str], str]:
    if ":" in vector_id:
        s, rest = vector_id.split(":", 1)
        return s, rest
    return None, vector_id


def _validate_tecnocasa_url(url: str) -> bool:
    if not url:
        return False
    return bool(TECNOCASA_DETAIL_RE.search(str(url).strip()))


def _validate_page_has_tecnocasa_payload(html: str) -> bool:
    if not html:
        return False
    if "estate-show-v2" not in html:
        return False
    soup = BeautifulSoup(html, "html.parser")
    comp = soup.find("estate-show-v2")
    return bool(comp and comp.get(":estate"))


def _validate_source_url(source: str, url: str) -> bool:
    if not url:
        return False
    pat = SOURCE_URL_PATTERNS.get(source)
    if not pat:
        return url.startswith("http")
    return bool(pat.search(str(url).strip()))


def _validate_page_structure(source: str, html: str) -> bool:
    if not html:
        return False
    s = source.lower()
    soup = BeautifulSoup(html, "html.parser")
    if s == "tecnocasa":
        return _validate_page_has_tecnocasa_payload(html)
    if s == "tunisieannonce":
        return bool(soup.find("td", class_="da_label_field") or soup.find("td", class_="da_field_text"))
    if s == "affare":
        return bool(soup.find(id="__NEXT_DATA__") or soup.find("div", class_=re.compile(r"Annonce_")))
    if s == "century21":
        return bool(
            soup.find("span", class_="item-price")
            or soup.find("div", id="property-description-wrap")
            or soup.find("ul", class_="list-2-cols")
        )
    if s == "darcom":
        return bool(soup.find("h1", class_="breadcrumbs-title") or soup.find("div", class_="pro-details-description"))
    if s == "mubawab":
        return bool(soup.find("span", class_="priceTag") or soup.find("div", class_="adBreadBlock"))
    if s == "newkey":
        return bool(soup.find("span", class_="item-price") or soup.find("ol", class_="breadcrumb"))
    if s == "verdar":
        return bool(soup.find("ul", class_="list-features") or soup.find("h1"))
    if s == "zitouna_immo":
        return bool(soup.find("div", class_="listing_single_description") or soup.find("div", class_="fp_price"))
    return True


def _call_scrape_detail(scraper: Any, url: str, metadata: Dict[str, Any]):
    fn = getattr(scraper, "_scrape_detail", None)
    if not callable(fn):
        return None
    try:
        import inspect

        sig = inspect.signature(fn)
        params = [p for p in sig.parameters.values() if p.name != "self"]
        kwargs: Dict[str, Any] = {}
        for p in params[1:]:
            if p.name == "transaction_type":
                kwargs[p.name] = metadata.get("transaction_type") or "Sale"
            elif p.name == "property_type":
                kwargs[p.name] = metadata.get("type") or "Other"
            else:
                kwargs[p.name] = metadata.get(p.name)
        return fn(url, **kwargs)
    except Exception:
        return None


def reprocess(
    source: Optional[str],
    limit: int,
    dry_run: bool,
    delete_invalid: bool,
) -> Dict[str, Any]:
    db = VectorDBHandler()
    scrapers = build_all_scrapers()
    scraper_by_source = {getattr(s, "source_name", s.__class__.__name__): s for s in scrapers}

    processed = 0
    updated = 0
    deleted = 0
    invalid_ids: List[str] = []
    skipped: List[str] = []
    failures: List[str] = []

    prefix = f"{source}:" if source else ""
    list_batch_size = 100
    fetch_batch_size = 10

    for ids_page in db.index.list(prefix=prefix, limit=list_batch_size):
        ids_list = list(ids_page) if not isinstance(ids_page, list) else ids_page
        if not ids_list:
            continue
        for i in range(0, len(ids_list), fetch_batch_size):
            batch_ids = ids_list[i : i + fetch_batch_size]
            result = db.index.fetch(ids=batch_ids)
            vectors = getattr(result, "vectors", None) or {}
            for vector_id, vector_data in vectors.items():
                if processed >= limit:
                    return {
                        "processed": processed,
                        "updated": updated,
                        "deleted": deleted,
                        "invalid_ids": invalid_ids,
                        "skipped": skipped,
                        "failures": failures,
                        "generated_at": _now_iso(),
                    }

                md = (getattr(vector_data, "metadata", None) or {}).copy()
                url = md.get("source_url") or md.get("url") or ""
                md_source = md.get("source_name")
                inferred_source, _ = _get_source_and_id(vector_id)
                source_name = md_source or inferred_source

                processed += 1

                if not source_name:
                    failures.append(vector_id)
                    continue

                # URL-level validation by source pattern
                if not _validate_source_url(source_name, url):
                    if delete_invalid:
                        invalid_ids.append(vector_id)
                    else:
                        skipped.append(vector_id)
                    continue

                # Fetch page and run a light structure validation
                scraper = scraper_by_source.get(source_name)
                if not scraper:
                    skipped.append(vector_id)
                    continue
                try:
                    resp = scraper._get_request(url)
                except Exception:
                    resp = None
                status = getattr(resp, "status_code", None)
                html = resp.text if resp is not None else ""
                if status not in (200, 201, 202) or not _validate_page_structure(source_name, html):
                    if delete_invalid:
                        invalid_ids.append(vector_id)
                    else:
                        skipped.append(vector_id)
                    continue

                listing = _call_scrape_detail(scraper, url, md)
                if not listing:
                    try:
                        resp = scraper._get_request(url)
                    except Exception:
                        resp = None
                    status = getattr(resp, "status_code", None)
                    if delete_invalid and status not in (200, 201, 202):
                        invalid_ids.append(vector_id)
                    else:
                        skipped.append(vector_id)
                    continue

                new_md = listing.to_vector_metadata()
                new_md.pop("id", None)
                merged = dict(md)
                merged.update(new_md)
                merged["last_update"] = _now_iso()
                merged["source_url"] = merged.get("url")

                clean_md = _clean_metadata(merged)

                if dry_run:
                    updated += 1
                    continue

                embedding = db._embed([listing.to_embedding_text()])[0]
                db.index.upsert(
                    vectors=[
                        {
                            "id": vector_id,
                            "values": embedding,
                            "metadata": clean_md,
                        }
                    ]
                )
                updated += 1

    if not dry_run and delete_invalid and invalid_ids:
        for i in range(0, len(invalid_ids), 1000):
            db.index.delete(ids=invalid_ids[i : i + 1000])
        deleted = len(invalid_ids)

    return {
        "processed": processed,
        "updated": updated,
        "deleted": deleted,
        "invalid_ids": invalid_ids[:200],
        "skipped": skipped[:200],
        "failures": failures[:200],
        "generated_at": _now_iso(),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", default=None)
    p.add_argument("--limit", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-delete-invalid", action="store_true")
    args = p.parse_args()

    report = reprocess(
        source=args.source,
        limit=args.limit,
        dry_run=args.dry_run,
        delete_invalid=not args.no_delete_invalid,
    )
    print(report)


if __name__ == "__main__":
    main()
