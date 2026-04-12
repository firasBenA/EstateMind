from bs4 import BeautifulSoup

from scrapers.all_scrapers import DarcomScraper


def test_darcom_extracts_price_surface_rooms_and_location_from_condition_block():
    html = """
    <div class="pro-details-condition">
      <div class="pro-details-condition-inner bg-gray">
        <ul class="condition-list">
          <li class="price">Ref1959a</li>
          <li class="price">400 000 DT</li>
          <li>Superficie terrain: 116 m²</li>
          <li>Superficie habitable: 116 m²</li>
          <li>Nb chambres: 2</li>
          <li>SDB: 1</li>
          <li>WC: 1</li>
          <li>Place parking: 1</li>
        </ul>
        <p>Ariana , La Soukra , La Soukra</p>
      </div>
    </div>
    """
    soup = BeautifulSoup(html, "html.parser")
    scraper = DarcomScraper()

    condition_ul = soup.find("ul", class_="condition-list")
    assert condition_ul is not None

    price = None
    surface = None
    rooms = None
    bathrooms = None
    features = []
    governorate = None
    municipalite = None
    city = None

    price_candidates = []
    for li in condition_ul.find_all("li"):
        text = li.get_text(strip=True)
        lower = text.lower()
        if li.get("class") and "price" in li.get("class"):
            if "ref" not in lower and scraper.parse_tunisian_price(text):
                price_candidates.append(text)
        if "superficie habitable" in lower and surface is None:
            surface = scraper.parse_surface(text)
        elif "superficie terrain" in lower and surface is None:
            surface = scraper.parse_surface(text)
        elif "m²" in text and surface is None:
            surface = scraper.parse_surface(text)
        if "nb chambres" in lower and rooms is None:
            import re

            m = re.search(r"\d+", text)
            rooms = int(m.group()) if m else None
        if ("sdb" in lower or ("salle" in lower and "bain" in lower)) and bathrooms is None:
            import re

            m = re.search(r"\d+", text)
            bathrooms = int(m.group()) if m else None
            if bathrooms is not None:
                features.append(f"SDB: {bathrooms}")
        if lower.startswith("wc"):
            import re

            m = re.search(r"\d+", text)
            wc = int(m.group()) if m else None
            if wc is not None:
                features.append(f"WC: {wc}")

    if price is None and price_candidates:
        price = scraper.parse_tunisian_price(price_candidates[0])

    loc_p = condition_ul.find_parent("div", class_="pro-details-condition")
    assert loc_p is not None
    p = loc_p.find("p")
    assert p is not None
    loc_text = p.get_text(" ", strip=True)
    parts = [x.strip() for x in loc_text.split(",") if x.strip()]
    if len(parts) >= 3:
        governorate = parts[0]
        municipalite = parts[1]
        city = parts[2]

    assert price == 400000.0
    assert surface == 116.0
    assert rooms == 2
    assert bathrooms == 1
    assert governorate == "Ariana"
    assert municipalite == "La Soukra"
    assert city == "La Soukra"
    assert "SDB: 1" in features
    assert "WC: 1" in features
