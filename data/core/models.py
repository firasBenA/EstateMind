"""
EstateMind core data models.
Schema matches the database exactly:
id, property_id, source_name, url, type, title, description, price, surface,
rooms, region, zone, city, municipalite, latitude, longitude, pdf_link,
images, features, scraped_at, last_update, transaction_type, currency,
raw_data_path, poi, image_count
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator
import uuid


class Location(BaseModel):
    governorate: Optional[str] = None   # → region column
    zone: Optional[str] = None          # → zone column (nord/east/west/south)
    city: Optional[str] = None          # → city column
    municipalite: Optional[str] = None  # → municipalite column
    district: Optional[str] = None
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @validator("governorate", "city", "municipalite", "district", "address", pre=True)
    def clean_str(cls, v):
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None


class POI(BaseModel):
    name: str
    category: str
    distance_m: Optional[float] = None


class PropertyListing(BaseModel):
    # Identification
    source_id: str                             # → property_id
    source_name: str
    url: str

    # Core attributes
    title: str
    description: Optional[str] = None
    price: Optional[float] = None
    currency: str = "TND"
    property_type: str = "Other"              # → type
    transaction_type: str = "Sale"

    # Location (flattened on save)
    location: Location

    # Details
    surface_area_m2: Optional[float] = None   # → surface
    rooms: Optional[int] = None
    bathrooms: Optional[int] = None
    floor: Optional[int] = None

    # Media
    images: List[str] = Field(default_factory=list)
    image_count: int = 0
    pdf_link: Optional[str] = None

    # Enrichment
    features: List[str] = Field(default_factory=list)
    pois: List[POI] = Field(default_factory=list)

    # Timestamps
    published_at: Optional[datetime] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    last_update: Optional[datetime] = None

    # Storage
    raw_data_path: Optional[str] = None

    # Internal (excluded from DB)
    raw_content: Optional[str] = Field(default=None, exclude=True)

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

    def model_post_init(self, __context):
        self.image_count = len(self.images)

    def to_db_dict(self) -> Dict[str, Any]:
        """Flat dict matching the database column schema exactly."""
        loc = self.location
        return {
            "property_id": self.source_id,
            "source_name": self.source_name,
            "url": self.url,
            "type": self.property_type,
            "title": self.title,
            "description": self.description,
            "price": self.price,
            "surface": self.surface_area_m2,
            "rooms": self.rooms,
            "region": loc.governorate,
            "zone": loc.zone,
            "city": loc.city,
            "municipalite": loc.municipalite,
            "latitude": loc.latitude,
            "longitude": loc.longitude,
            "pdf_link": self.pdf_link,
            "images": self.images,
            "features": self.features,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "transaction_type": self.transaction_type,
            "currency": self.currency,
            "raw_data_path": self.raw_data_path,
            "poi": [p.model_dump() for p in self.pois],
            "image_count": self.image_count,
        }

    def to_embedding_text(self) -> str:
        """Text used to generate the vector embedding."""
        loc = self.location
        parts = [
            f"Propriété: {self.title}",
            f"Type: {self.property_type} pour {self.transaction_type}",
            f"Localisation: {loc.city or ''} {loc.municipalite or ''} {loc.governorate or ''}",
            f"Prix: {self.price} {self.currency}" if self.price else "",
            f"Surface: {self.surface_area_m2} m²" if self.surface_area_m2 else "",
            f"Chambres: {self.rooms}" if self.rooms else "",
            f"Caractéristiques: {', '.join(self.features)}" if self.features else "",
            f"Description: {self.description or ''}",
        ]
        return " ".join(p for p in parts if p)

    def to_vector_metadata(self) -> Dict[str, Any]:
        """
        Payload for Pinecone / Qdrant point — all filterable fields.
        Schema matches the core database structure:
        id, property_id, source_name, url, type, title, description, price, surface, 
        rooms, region, zone, city, municipalite, latitude, longitude, images, 
        image_count, features, scraped_at, last_update, transaction_type, currency, poi
        """
        loc = self.location
        return {
            "id": f"{self.source_name}:{self.source_id}",
            "property_id": self.source_id,
            "source_name": self.source_name,
            "url": self.url,
            "type": self.property_type,
            "title": self.title,
            "description": self.description or "",
            "price": self.price,
            "surface": self.surface_area_m2,
            "rooms": self.rooms,
            "region": loc.governorate,
            "zone": loc.zone,
            "city": loc.city,
            "municipalite": loc.municipalite,
            "latitude": loc.latitude,
            "longitude": loc.longitude,
            "images": self.images,
            "image_count": self.image_count,
            "features": self.features,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
            "last_update": self.last_update.isoformat() if self.last_update else datetime.utcnow().isoformat(),
            "transaction_type": self.transaction_type,
            "currency": self.currency,
            "poi": [p.name for p in self.pois],
        }


class RawData(BaseModel):
    source_id: str
    source_name: str
    content: str
    file_type: str = "html"
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
