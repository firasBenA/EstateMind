# ai_service/pipeline.py

import logging
from YoloExtractor import YOLOExtractor
from ClipDetector import CLIPDetector
from llm_generator import LLMGenerator  # ✅ Import LLMGenerator

logger = logging.getLogger("pipeline")


class DescriptionPipeline:
    def __init__(self, registry):
        self.yolo_extractor = YOLOExtractor(registry.yolo)
        self.clip_detector = CLIPDetector(registry.clip_model, registry.clip_processor, registry.device)
        
        # ✅ Initialize LLMGenerator with new properties
        self.llm_generator = LLMGenerator(
            registry.llm_model, 
            registry.llm_tokenizer, 
            registry.device
        )

    def run(self, images, metadata):
        logger.info("Starting pipeline...")
        
        # 1. Extract visual features
        yolo_features = self.yolo_extractor.extract(images)
        clip_features = self.clip_detector.detect(images)
        
        # 2. Generate text
        result = self.llm_generator.generate(yolo_features, clip_features, metadata)
        
        logger.info("Pipeline completed.")
        return result