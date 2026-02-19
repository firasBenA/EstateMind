import sys
from loguru import logger
from config.settings import settings

def setup_logging():
    logger.remove()  # Remove default handler
    
    # Console handler
    logger.add(
        sys.stderr,
        level=settings.LOG_LEVEL,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    # File handler for errors
    logger.add(
        "logs/errors.log",
        rotation="10 MB",
        retention="1 week",
        level="ERROR",
        compression="zip"
    )
    
    # File handler for all logs
    logger.add(
        "logs/app.log",
        rotation="1 day",
        retention="1 month",
        level="INFO"
    )

    return logger

log = setup_logging()
