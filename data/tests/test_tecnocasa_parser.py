import json
from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.all_scrapers import TecnocasaScraper


def _make_scraper_without_init() -> TecnocasaScraper:
    scraper = TecnocasaScraper.__new__(TecnocasaScraper)
    scraper.source_name = "tecnocasa"
    scraper.base_url = "https://www.tecnocasa.tn"
    return scraper


def test_tecnocasa_extracts_estate_json_from_fixture():
    repo_root = Path(__file__).resolve().parents[2]
    html = (repo_root / "tecnocasa.html").read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    scraper = _make_scraper_without_init()
    estate = scraper._extract_estate_json(soup)
    assert isinstance(estate, dict)
    assert estate.get("id") == 36522
    assert estate.get("numeric_price") == 211420
    assert estate.get("previous_price") == "221 650 DT"
    assert estate.get("province", {}).get("title") == "Cap Bon"
    assert "points_of_interest" in estate


def test_tecnocasa_parses_listing_fields_from_estate_json():
    repo_root = Path(__file__).resolve().parents[2]
    html = (repo_root / "tecnocasa.html").read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    scraper = _make_scraper_without_init()
    estate = scraper._extract_estate_json(soup)
    listing = scraper._parse_estate_json(estate, "https://www.tecnocasa.tn/vendre/terrain/cap-bon/hammamet/36522.html", "Sale")

    assert listing.source_id == "36522"
    assert listing.price == 211420
    assert listing.surface_area_m2 == 341.0
    assert listing.location.city == "Hammamet"
    assert listing.location.governorate == "Cap Bon"
    assert isinstance(listing.features, list)


def test_tecnocasa_poi_json_is_valid():
    repo_root = Path(__file__).resolve().parents[2]
    html = (repo_root / "tecnocasa.html").read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    scraper = _make_scraper_without_init()
    estate = scraper._extract_estate_json(soup)
    poi = estate.get("points_of_interest")
    assert isinstance(poi, dict)
    dumped = json.dumps(poi, ensure_ascii=False)
    assert isinstance(dumped, str) and dumped
