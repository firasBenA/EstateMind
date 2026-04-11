# tools/process_existing_images.py
"""
Generate image embeddings for existing listings.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database.vector_db import VectorDBHandler
from loguru import logger

def main():
    db = VectorDBHandler()
    
    logger.info("Processing existing images...")
    processed = db.process_existing_images(limit=5000)  # Process up to 500
    
    stats = db.get_stats()
    logger.info(f"Complete! Stats: {stats}")
    
    db.close()

if __name__ == "__main__":
    main()