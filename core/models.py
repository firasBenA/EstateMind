from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, HttpUrl, validator


class Location(BaseModel):
    governorate: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @validator("governorate", "city", "district", "address", pre=True)
    def normalize_text(cls, v):
        if v is None:
            return None
        value = str(v).strip()
        if not value:
            return None
        return value


class POI(BaseModel):
    name: str
    category: str
    distance_m: Optional[float] = None

class PropertyListing(BaseModel):
    # Identification
    source_id: str
    source_name: str
    url: str
    
    # Core attributes
    title: str
    description: Optional[str] = None
    price: Optional[float] = None
    currency: str = "TND"
    property_type: str  # Apartment, Villa, Land, Office, etc.
    transaction_type: str # Sale, Rent
    
    # Details
    location: Location
    surface_area_m2: Optional[float] = None
    rooms: Optional[int] = None
    bathrooms: Optional[int] = None
    floor: Optional[int] = None
    
    images: List[str] = Field(default_factory=list)
    
    # Metadata
    features: List[str] = Field(default_factory=list)
    pois: List[POI] = Field(default_factory=list)
    published_at: Optional[datetime] = None
    scraped_at: datetime = Field(default_factory=datetime.now)
    
    # Storage references
    raw_data_path: Optional[str] = None
    
    # Internal use only (not stored in DB)
    raw_content: Optional[str] = Field(default=None, exclude=True)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class RawData(BaseModel):
    source_id: str
    source_name: str
    content: str  # HTML, JSON, or Text content
    file_type: str = "html" # html, json, pdf_text
    scraped_at: datetime = Field(default_factory=datetime.now)
