# data/generator/model_registry.py

import logging
import os
import gc
import torch

logger = logging.getLogger("model_registry")


class ModelRegistry:
    def __init__(self):
        self.device: str = "cpu" # Or "cuda" if you have enough VRAM
        self._vl_model = None
        self._vl_processor = None
        self._ready = False

    @property
    def vl_model(self):
        return self._vl_model

    @property
    def vl_processor(self):
        return self._vl_processor

    def is_ready(self) -> bool:
        return self._ready

    def load_all(self):
        self._detect_device()
        # ✅ LOAD ONLY QWEN2-VL (It replaces YOLO + CLIP + LLM)
        self._load_qwen_vl()
        self._ready = True
        logger.info(f"ModelRegistry: Qwen2-VL loaded on {self.device}")

    def unload_all(self):
        self._vl_model = None
        self._vl_processor = None
        self._ready = False
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    def _detect_device(self):
        if torch.cuda.is_available():
            self.device = "cuda"
            logger.info(f"GPU detected: {torch.cuda.get_device_name(0)}")
        else:
            self.device = "cpu"
            logger.info("No GPU detected — running on CPU")

    def _load_llm(self):
        """Load Qwen2-VL-2B-Instruct (Vision-Language) with 4-bit quantization."""
        try:
            from transformers import AutoProcessor, Qwen2VLForConditionalGeneration, BitsAndBytesConfig
            
            # ✅ USE VISION-LANGUAGE MODEL
            model_id = "Qwen/Qwen2-VL-2B-Instruct"
            cache_dir = os.environ.get("HF_CACHE_DIR", None)
            logger.info(f"Loading VLM: {model_id} on {self.device}…")

            # Load processor (handles both text and images)
            self._llm_tokenizer = AutoProcessor.from_pretrained(
                model_id, 
                cache_dir=cache_dir, 
                trust_remote_code=True
            )

            # Configure 4-bit quantization for low VRAM
            if self.device == "cuda":
                bnb_config = BitsAndBytesConfig(
                    load_in_4bit=True,
                    bnb_4bit_compute_dtype=torch.float16,
                    bnb_4bit_use_double_quant=True,
                    bnb_4bit_quant_type="nf4",
                )
                load_kwargs = dict(
                    cache_dir=cache_dir,
                    trust_remote_code=True,
                    device_map="auto",
                    quantization_config=bnb_config,
                )
                logger.info("Using 4-bit quantization for VLM on GPU")
            else:
                # CPU fallback
                load_kwargs = dict(
                    cache_dir=cache_dir,
                    trust_remote_code=True,
                    device_map="cpu",
                    torch_dtype=torch.float32,
                )

            # Load the VLM model
            self._llm_model = Qwen2VLForConditionalGeneration.from_pretrained(
                model_id, 
                **load_kwargs
            )
            self._llm_model.eval()
            logger.info("✅ VLM (Qwen2-VL-2B) loaded successfully")
            
        except Exception as exc:
            logger.error(f"VLM load failed: {exc}", exc_info=True)
            raise RuntimeError(f"Cannot load VLM: {exc}") from exc

    def _load_qwen_vl(self):
        """Load Qwen2-VL-2B-Instruct."""
        try:
            from transformers import AutoProcessor, Qwen2VLForConditionalGeneration
            
            model_id = "Qwen/Qwen2-VL-2B-Instruct"
            cache_dir = os.environ.get("HF_CACHE_DIR", None)
            logger.info(f"Loading Qwen2-VL: {model_id}…")

            # Load Processor
            self._vl_processor = AutoProcessor.from_pretrained(
                model_id, 
                cache_dir=cache_dir, 
                trust_remote_code=True
            )

            # Load Model
            # Use float16 if on GPU, float32 if on CPU
            dtype = torch.float16 if self.device == "cuda" else torch.float32
            
            self._vl_model = Qwen2VLForConditionalGeneration.from_pretrained(
                model_id,
                torch_dtype=dtype,
                device_map="auto" if self.device == "cuda" else "cpu",
                trust_remote_code=True,
                low_cpu_mem_usage=True, # Helps with memory
            )
            self._vl_model.eval()
            logger.info("✅ Qwen2-VL loaded successfully")
            
        except Exception as exc:
            logger.error(f"Qwen2-VL load failed: {exc}", exc_info=True)
            raise RuntimeError(f"Cannot load Qwen2-VL: {exc}") from exc

