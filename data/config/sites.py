"""
EstateMind — Verified site URLs and scraper configurations.
"""

SITE_CONFIGS = {
    "century21": {
        "base_url": "https://century21.tn",
        "list_urls": [
            "https://century21.tn/status/vente-immobilier-tunisie/",
            "https://century21.tn/status/location-immobilier-tunisie/",
        ],
        "needs_js": True,
        "engine": "selenium",
        "min_delay": 4.0,
        "max_delay": 8.0,
        "max_pages": 50,
        "link_pattern": "/property/",
        "pagination_template": "{base_url}page/{page}/",
        "notes": "WordPress/Houzez. CAPTCHA on fast requests. Selenium required.",
    },
    "zitouna_immo": {
        "base_url": "https://www.zitounaimmo.com",
        "list_urls": [
            "https://www.zitounaimmo.com/acheter",
            "https://www.zitounaimmo.com/louer",
        ],
        "needs_js": False,
        "engine": "requests",
        "min_delay": 4.0,
        "max_delay": 8.0,
        "max_pages": 50,
        "link_pattern": "/bien/details/",
        "pagination_template": "{cat_url}?page={page}",
        "notes": "Houzez. Rate limits on fast requests. Long delay needed.",
    },
    "mubawab": {
        "base_url": "https://www.mubawab.tn",
        "list_urls": [
            "https://www.mubawab.tn/immobilier-vendre/appartements",
            "https://www.mubawab.tn/immobilier-vendre/maisons-et-villas",
            "https://www.mubawab.tn/immobilier-vendre/terrains-et-fermes",
            "https://www.mubawab.tn/immobilier-vendre/bureaux-et-commerces",
            "https://www.mubawab.tn/immobilier-louer/appartements",
            "https://www.mubawab.tn/immobilier-louer/maisons-et-villas",
            "https://www.mubawab.tn/immobilier-louer/bureaux-et-commerces",
        ],
        "needs_js": False,
        "engine": "requests",
        "min_delay": 2.0,
        "max_delay": 5.0,
        "max_pages": 20,
        "link_pattern": "/fr/",
        "pagination_template": "{cat_url}?page={page}",
        "notes": "Largest Tunisian portal. Good HTML structure. Use fr/ prefix.",
    },
    "newkey": {
        "base_url": "https://www.newkey.com.tn",
        "list_urls": [
            "https://www.newkey.com.tn/acheter",
            "https://www.newkey.com.tn/louer",
        ],
        "needs_js": True,
        "engine": "selenium",
        "min_delay": 2.0,
        "max_delay": 4.0,
        "max_pages": 50,
        "link_pattern": "/bien/details/",
        "pagination_template": "{cat_url}?page={page}",
        "notes": "Houzez. React frontend. Selenium needed for full hydration.",
    },
    "darcom": {
        "base_url": "https://www.darcomtunisia.com",
        "list_urls": [
            "https://www.darcomtunisia.com/vente",
            "https://www.darcomtunisia.com/location",
        ],
        "needs_js": True,
        "engine": "selenium",
        "min_delay": 2.0,
        "max_delay": 4.0,
        "max_pages": 50,
        "link_pattern": "/bien/details/",
        "pagination_template": "{cat_url}?page={page}",
        "notes": "Houzez. Correct domain is darcomtunisia.com",
    },
    "verdar": {
        "base_url": "https://www.verdar.tn",
        "list_urls": [
            "https://www.verdar.tn/acheter",
            "https://www.verdar.tn/louer",
        ],
        "needs_js": False,
        "engine": "requests",
        "min_delay": 2.0,
        "max_delay": 4.0,
        "max_pages": 50,
        "link_pattern": "/bien/details/",
        "pagination_template": "{cat_url}?page={page}",
        "notes": "Houzez. /acheter and /louer are the correct list URLs.",
    },
    "tunisieannonce": {
        "base_url": "http://www.tunisie-annonce.com",
        "list_urls": [
            "http://www.tunisie-annonce.com/AnnoncesImmobilier.asp",
        ],
        "needs_js": False,
        "engine": "requests",
        "min_delay": 2.0,
        "max_delay": 4.0,
        "max_pages": 50,
        "link_pattern": "AnnoncesImmobilier",
        "pagination_template": "{base_url}?rech_page_num={page}",
        "notes": "Old ASP.NET site. HTTP (not HTTPS). Separate subdomain.",
    },
    "tecnocasa": {
        "base_url": "https://www.tecnocasa.tn",
        "list_urls": [
            "https://www.tecnocasa.tn/vente-immobilier-tunisie/",
            "https://www.tecnocasa.tn/location-immobilier-tunisie/",
        ],
        "needs_js": True,
        "engine": "selenium",
        "min_delay": 3.0,
        "max_delay": 6.0,
        "max_pages": 30,
        "link_pattern": "/property/",
        "pagination_template": "{base_url}page/{page}/",
        "notes": "WordPress/Houzez. French URL paths. Old Italian paths 404.",
    },
    "affare": {
        "base_url": "https://www.affare.tn",
        "list_urls": [
            "https://www.affare.tn/petites-annonces/tunisie/immobilier",
        ],
        "needs_js": False,
        "engine": "requests",
        "min_delay": 2.0,
        "max_delay": 4.0,
        "max_pages": 30,
        "link_pattern": "/annonce/",
        "pagination_template": "{cat_url}?page={page}",
        "notes": "Next.js. __NEXT_DATA__ JSON in page. Correct list URL uses lowercase.",
    },
}

def get_config(site_name: str) -> dict:
    return SITE_CONFIGS.get(site_name, {})

def get_all_configs() -> dict:
    return SITE_CONFIGS
