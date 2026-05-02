#!/usr/bin/env python
"""Quick test of search extraction and results."""
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from agent.tools.search import search_listings

# Test 1: Direct search with parameters
print("=== Test 1: Direct search with min_surface ===")
result = search_listings(city='Ariana', min_surface=200, page_size=10)
print(f"Found {result['count']} listings in Ariana with 200m² or more")
if result['results']:
    for i, listing in enumerate(result['results'][:3], 1):
        surface = listing.get('surface', 'N/A')
        price = listing.get('price', 0)
        price_str = f"{price:,.0f} TND" if price else "N/A"
        print(f"{i}. {listing.get('title', 'N/A')[:40]} - {surface}m² - {price_str}")
else:
    print("No results found")

# Test 2: Parameter extraction
print("\n=== Test 2: Parameter extraction ===")
from agent.agent import AgentOrchestrator

# Create agent without LangChain (it will fail but that's okay)
try:
    agent = AgentOrchestrator()
except Exception:
    # Agent init failed due to LangChain, but we can still use methods
    pass

# Manually test extraction (without initializing agent)
import re
msg = "I want houses in ariena with 200m² or more"
msg_low = msg.lower()

# City alias
city_aliases = {"ariena": "Ariana"}
city = None
for alias, canonical in city_aliases.items():
    if alias in msg_low:
        city = canonical
        break
print(f"City: {city}")

# Surface extraction
surface_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m2|m²|sqm|square\s*meters?)', msg_low)
if surface_match:
    surface_val = float(surface_match.group(1))
    min_keywords = ["at least", "minimum", "min", "more than", "above", "bigger", "over", "or more"]
    is_min_surface = any(w in msg_low for w in min_keywords)
    print(f"Surface: {surface_val}m²")
    print(f"Is min_surface: {is_min_surface}")
    if is_min_surface:
        print(f"→ Will search for min_surface = {surface_val}")
