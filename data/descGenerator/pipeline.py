# data/generator/pipeline.py

import logging
from PIL import Image

logger = logging.getLogger("pipeline")

class DescriptionPipeline:
    def __init__(self, registry):
        # ✅ We only need the VL model now
        self.vl_model = registry.vl_model
        self.vl_processor = registry.vl_processor
        self.device = registry.device

    def run(self, images: list[Image.Image], metadata: dict):
        logger.info("Starting Qwen2-VL Pipeline...")
        
        # Call the generator directly with images and metadata
        result = generate_description_vl(
            self.vl_model, 
            self.vl_processor, 
            images, 
            metadata,
            self.device
        )
        
        logger.info("Pipeline completed.")
        return result