# data/generator/model_registry.py

import logging
import os
import gc
import torch

logger = logging.getLogger("model_registry")


class ModelRegistry:
    def __init__(self):
        self.device: str = "cpu"
        self._yolo = None
        self._clip_model = None
        self._clip_processor = None
        
        # ✅ Initialize LLM attributes
        self._llm_model = None
        self._llm_tokenizer = None
        
        self._ready = False

    # ── Public accessors ──────────────────────────────────────────────────────

    @property
    def yolo(self):
        return self._yolo

    @property
    def clip_model(self):
        return self._clip_model

    @property
    def clip_processor(self):
        return self._clip_processor

    @property
    def llm_model(self):
        return self._llm_model

    @property
    def llm_tokenizer(self):
        return self._llm_tokenizer

    def is_ready(self) -> bool:
        return self._ready

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def load_all(self):
        self._detect_device()
        self._load_yolo()
        self._load_clip()
        self._load_llm()  # ✅ Calls the function defined below
        self._ready = True
        logger.info(f"ModelRegistry: all models loaded on {self.device}")

    def unload_all(self):
        self._yolo = None
        self._clip_model = None
        self._clip_processor = None
        self._llm_model = None
        self._llm_tokenizer = None
        self._ready = False
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        logger.info("ModelRegistry: all models unloaded")

    # ── Device detection ──────────────────────────────────────────────────────

    def _detect_device(self):
        if torch.cuda.is_available():
            self.device = "cuda"
            gpu = torch.cuda.get_device_name(0)
            vram = torch.cuda.get_device_properties(0).total_memory / 1e9
            logger.info(f"GPU detected: {gpu} ({vram:.1f} GB VRAM)")
        elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            self.device = "mps"
            logger.info("Apple MPS detected")
        else:
            self.device = "cpu"
            logger.info("No GPU detected — running on CPU (inference will be slower)")

    # ── YOLO ──────────────────────────────────────────────────────────────────

    def _load_yolo(self):
        try:
            from ultralytics import YOLO
            model_path = os.environ.get("YOLO_MODEL_PATH", "yolov8n.pt")
            logger.info(f"Loading YOLO from {model_path}…")
            self._yolo = YOLO(model_path)
            import numpy as np
            dummy = np.zeros((640, 640, 3), dtype=np.uint8)
            self._yolo(dummy, verbose=False)
            logger.info("✅ YOLO loaded & warmed up")
        except Exception as exc:
            logger.warning(f"YOLO load failed: {exc}")

    # ── CLIP ──────────────────────────────────────────────────────────────────

    def _load_clip(self):
        try:
            from transformers import CLIPProcessor, CLIPModel
            model_id = os.environ.get("CLIP_MODEL_ID", "openai/clip-vit-base-patch32")
            cache_dir = os.environ.get("HF_CACHE_DIR", None)
            logger.info(f"Loading CLIP {model_id}…")
            self._clip_processor = CLIPProcessor.from_pretrained(model_id, cache_dir=cache_dir)
            self._clip_model = CLIPModel.from_pretrained(
                model_id, 
                cache_dir=cache_dir, 
                torch_dtype=torch.float16 if self.device == "cuda" else torch.float32
            ).to(self.device)
            self._clip_model.eval()
            logger.info("✅ CLIP loaded")
        except Exception as exc:
            logger.warning(f"CLIP load failed: {exc}")

    # ── LLM (Qwen2-0.5B) ──────────────────────────────────────────────────────
    # ✅ SWITCHED TO 0.5B MODEL TO FIX MEMORY ERROR

    def _load_llm(self):
        """Load Qwen2-0.5B-Instruct explicitly on CPU."""
        try:
            from transformers import AutoTokenizer, AutoModelForCausalLM
            
            # ✅ USE SMALLER MODEL
            model_id = "Qwen/Qwen2-0.5B-Instruct" 
            
            cache_dir = os.environ.get("HF_CACHE_DIR", None)
            logger.info(f"Loading LLM: {model_id} on {self.device}…")

            self._llm_tokenizer = AutoTokenizer.from_pretrained(
                model_id, 
                cache_dir=cache_dir, 
                trust_remote_code=True
            )

            # ✅ FORCE CPU LOAD to avoid meta tensor errors and save RAM
            load_kwargs = dict(
                cache_dir=cache_dir,
                trust_remote_code=True,
                device_map="cpu", 
                torch_dtype=torch.float32, 
            )

            self._llm_model = AutoModelForCausalLM.from_pretrained(
                model_id, 
                **load_kwargs
            )
            self._llm_model.eval()
            logger.info("✅ LLM (Qwen2-0.5B) loaded successfully")
            
        except Exception as exc:
            logger.error(f"LLM load failed: {exc}", exc_info=True)
            raise RuntimeError(f"Cannot load LLM: {exc}") from exc