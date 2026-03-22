# config/settings.py
import os
from pathlib import Path
from pydantic_settings import BaseSettings
from dotenv import load_dotenv


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    # MongoDB
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME: str = os.getenv("DB_NAME", "estatemind")
    
    # Paths
    RAW_DATA_PATH: Path = Path(os.getenv("RAW_DATA_PATH", BASE_DIR / "data" / "raw"))
    LOG_DIR: str = os.getenv("LOG_DIR", "./logs")
    EXPORT_DIR: str = os.getenv("EXPORT_DIR", "data/exports")
    
    # PostgreSQL
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "estatemind")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "capTEEMO500")
    
    # Pinecone
    PINECONE_API_KEY: str = os.getenv("PINECONE_API_KEY", "")
    PINECONE_INDEX_NAME: str = os.getenv("PINECONE_INDEX_NAME", "property-listings")
    PINECONE_CLOUD: str = os.getenv("PINECONE_CLOUD", "aws")
    PINECONE_REGION: str = os.getenv("PINECONE_REGION", "us-east-1")
    
    # Embeddings & AI
    EMBEDDING_STRATEGY: str = os.getenv("EMBEDDING_STRATEGY", "huggingface")
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    
    # OpenRouter (NEW)
    OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")
    OPENROUTER_MODEL: str = os.getenv("OPENROUTER_MODEL", "mistralai/mistral-small-2603")
    
    # Time series database (NEW)
    TIMESERIES_DB_PATH: str = os.getenv("TIMESERIES_DB_PATH", "data/estatemind_timeseries.db")
    
    # Scraping configurations
    MAX_PAGES_PER_SITE: int = int(os.getenv("MAX_PAGES_PER_SITE", "50"))
    HEADLESS: bool = os.getenv("HEADLESS", "true").lower() == "true"
    USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    REQUEST_TIMEOUT: int = 30
    DELAY_MIN: int = 1
    DELAY_MAX: int = 3
    REQUEST_RETRIES: int = int(os.getenv("REQUEST_RETRIES", "3"))
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Data quality thresholds (NEW)
    MIN_PRICE_SALE: int = int(os.getenv("MIN_PRICE_SALE", "5000"))
    MAX_PRICE_SALE: int = int(os.getenv("MAX_PRICE_SALE", "50000000"))
    MIN_PRICE_RENT: int = int(os.getenv("MIN_PRICE_RENT", "100"))
    MAX_PRICE_RENT: int = int(os.getenv("MAX_PRICE_RENT", "50000"))
    MIN_SURFACE: int = int(os.getenv("MIN_SURFACE", "10"))
    MAX_SURFACE: int = int(os.getenv("MAX_SURFACE", "8000"))

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # This allows extra fields in .env without error
        case_sensitive = False


# Create settings instance
settings = Settings()

# Ensure directories exist
settings.RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
Path(settings.LOG_DIR).mkdir(parents=True, exist_ok=True)
Path(settings.EXPORT_DIR).mkdir(parents=True, exist_ok=True)
Path(settings.TIMESERIES_DB_PATH).parent.mkdir(parents=True, exist_ok=True)