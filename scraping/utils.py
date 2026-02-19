import re
from typing import Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

def clean_text(s: str) -> str:
    return " ".join(s.split()).strip()

def parse_price(text: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Very basic. You will adapt based on each site format:
    examples: "1 200 DT", "850000 TND", "€ 1200"
    """
    if not text:
        return None, None
    t = text.replace("\xa0", " ")
    currency = None
    if "DT" in t or "TND" in t:
        currency = "TND"
    elif "€" in t or "EUR" in t:
        currency = "EUR"
    elif "$" in t or "USD" in t:
        currency = "USD"

    numbers = re.findall(r"[\d\s.,]+", t)
    if not numbers:
        return None, currency

    num = numbers[0].strip().replace(" ", "").replace(",", ".")
    try:
        return float(num), currency
    except:
        return None, currency

def safe_int(text: str) -> Optional[int]:
    m = re.search(r"\d+", text or "")
    return int(m.group()) if m else None

def safe_float(text: str) -> Optional[float]:
    m = re.search(r"(\d+(\.\d+)?)", (text or "").replace(",", "."))
    return float(m.group(1)) if m else None

def canonical_url(url: str) -> str:
    """
    Remove query string + fragments and normalize trailing slash.
    Example:
      https://site.com/x?a=1#t -> https://site.com/x
    """
    parts = urlsplit(url)
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    if clean.endswith("/") and len(parts.path) > 1:
        clean = clean.rstrip("/")
    return clean


