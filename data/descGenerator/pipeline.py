# data/descGenerator/pipeline.py

import logging
from llm_generator import LLMGenerator

logger = logging.getLogger("pipeline")


class DescriptionPipeline:
    def __init__(self, registry):
        # ✅ Qwen2-VL does everything, so we only need the VLM generator
        self.vl_generator = LLMGenerator(
            registry.vl_model,
            registry.vl_processor,
            registry.device
        )

    def run(self, images, metadata):
        logger.info("Starting Qwen2-VL pipeline...")
        
        # ✅ Qwen2-VL handles visual analysis + text generation in one step
        result = self.vl_generator.generate(
            images=images,
            metadata=metadata
        )
        
        logger.info("Qwen2-VL Pipeline completed.")
        return result