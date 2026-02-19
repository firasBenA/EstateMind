import os
from pathlib import Path
from pydantic_settings import BaseSettings
from dotenv import load_dotenv


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME: str = os.getenv("DB_NAME", "estatemind")
    RAW_DATA_PATH: Path = Path(os.getenv("RAW_DATA_PATH", BASE_DIR / "data" / "raw"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "estatemind")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "capTEEMO500")
    
    # Scraper configurations
    USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    REQUEST_TIMEOUT: int = 30
    DELAY_MIN: int = 1
    DELAY_MAX: int = 3
    REQUEST_RETRIES: int = int(os.getenv("REQUEST_RETRIES", "3"))

    class Config:
        env_file = ".env"

settings = Settings()

# Ensure directories exist
settings.RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
