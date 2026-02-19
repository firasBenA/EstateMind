from pydantic import BaseModel, HttpUrl
from typing import Optional, Literal,List
from datetime import datetime
from pydantic import Field

TransactionType = Literal["rent", "sale"]

class SearchParams(BaseModel):
    transaction: TransactionType
    governorate: Optional[str] = None
    city: Optional[str] = None
    property_type: Optional[str] = None
    max_pages: int = 10

class Listing(BaseModel):
    source: str
    source_listing_id: str  # ✅ NEW (dedup stable)

    transaction: TransactionType
    url: HttpUrl
    image_urls: List[str] = Field(default_factory=list)


    title: str
    price: Optional[float] = None
    currency: Optional[str] = None

    governorate: Optional[str] = None
    city: Optional[str] = None
    zone: Optional[str] = None #### NEW
    property_type: Optional[str] = None

    surface_m2: Optional[float] = None
    rooms: Optional[int] = None
    bathrooms: Optional[int] = None
    poi: Optional[dict] = None ### NEW: points of interest, e.g. {"school": 500, "metro": 300} (distances in meters)

    posted_at: Optional[datetime] = None
    scraped_at: datetime = datetime.utcnow()

    lat: Optional[float] = None
    lon: Optional[float] = None

    
