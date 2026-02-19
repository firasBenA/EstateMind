import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from core.pipeline import ScrapingPipeline
from core.models import PropertyListing, Location, POI


def make_pipeline() -> ScrapingPipeline:
    pipeline = ScrapingPipeline.__new__(ScrapingPipeline)
    return pipeline


def make_listing(location: Location, pois=None) -> PropertyListing:
    return PropertyListing(
        source_id="1",
        source_name="test",
        url="http://example.com",
        title="Test",
        property_type="Apartment",
        transaction_type="Sale",
        location=location,
        pois=pois or [],
    )


def test_location_missing_rejected():
    pipeline = make_pipeline()
    listing = make_listing(Location())
    assert pipeline._validate_listing(listing) is False


def test_location_with_city_accepted_and_normalized():
    pipeline = make_pipeline()
    listing = make_listing(Location(city=" tunis "))
    assert pipeline._validate_listing(listing) is True
    assert listing.location.city == "tunis"


def test_pois_validated_and_normalized():
    pipeline = make_pipeline()
    pois = [
        POI(name="Lycee", category="School"),
        {"name": "CHU", "category": "Hospital"},
        {"name": "", "category": "School"},
    ]
    listing = make_listing(Location(city="Tunis"), pois=pois)
    assert pipeline._validate_listing(listing) is True
    assert len(listing.pois) == 2
    for poi in listing.pois:
        assert poi.category in {"school", "hospital", "shopping", "restaurant", "transport", "other"}
