"""
EstateMind AI Microservice
FastAPI service that handles image-based real estate description generation.
Models load ONCE at startup via lifespan context manager.

POST /generate-description
  - multipart/form-data: images (1-3 files) + metadata (JSON string)
  - returns: { bullets, highlights, tone }
"""

from contextlib import asynccontextmanager
import logging
import json
import os
import io
import time

from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from PIL import Image

from ModelRegistry import ModelRegistry
from pipeline import DescriptionPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ai_service")

# ── Global model registry (loaded once) ──────────────────────────────────────
registry = ModelRegistry()
pipeline: DescriptionPipeline | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load all models at startup, release on shutdown."""
    global pipeline
    logger.info("🚀 Loading AI models…")
    t0 = time.time()
    try:
        registry.load_all()
        pipeline = DescriptionPipeline(registry)
        logger.info(f"✅ All models ready in {time.time() - t0:.1f}s")
    except Exception as exc:
        logger.error(f"❌ Model loading failed: {exc}", exc_info=True)
        raise
    yield
    logger.info("🛑 Shutting down AI service")
    registry.unload_all()


app = FastAPI(
    title="EstateMind AI Service",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get(
        "CORS_ORIGINS",
        "http://localhost:5173,http://localhost:3000,http://localhost:8000",
    ).split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status": "ok",
        "models_loaded": registry.is_ready(),
        "device": registry.device,
    }


# ── Main endpoint ─────────────────────────────────────────────────────────────

@app.post("/generate-description")
async def generate_description(
    images: list[UploadFile] = File(..., description="1–3 property images"),
    metadata: str = Form(..., description="JSON string with property metadata"),
):
    """
    Accept 1-3 property images + metadata JSON, return AI-generated description.

    metadata shape:
    {
      "type": "apartment",         // property type
      "city": "Tunis",
      "price": 350000,
      "currency": "TND",
      "surface": 120,
      "rooms": 3,
      "transaction": "sale",
      "tone": "professional"       // optional: professional | luxury | casual
    }
    """
    if not pipeline:
        raise HTTPException(status_code=503, detail="Models not loaded yet")

    # ── Validate image count ──────────────────────────────────────────────
    if len(images) == 0:
        raise HTTPException(status_code=400, detail="At least 1 image is required")
    if len(images) > 3:
        raise HTTPException(status_code=400, detail="Maximum 3 images allowed")

    # ── Parse metadata ────────────────────────────────────────────────────
    try:
        meta = json.loads(metadata)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="metadata must be valid JSON")

    # ── Decode images ─────────────────────────────────────────────────────
    pil_images: list[Image.Image] = []
    for upload in images:
        if upload.content_type not in ("image/jpeg", "image/png", "image/webp"):
            raise HTTPException(
                status_code=415,
                detail=f"Unsupported image type: {upload.content_type}. Use JPEG/PNG/WEBP.",
            )
        raw = await upload.read()
        if len(raw) > 10 * 1024 * 1024:  # 10 MB guard
            raise HTTPException(status_code=413, detail=f"Image {upload.filename} exceeds 10 MB")
        try:
            img = Image.open(io.BytesIO(raw)).convert("RGB")
            pil_images.append(img)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Cannot decode image: {upload.filename}")

    # ── Run pipeline ──────────────────────────────────────────────────────
    try:
        t0 = time.time()
        result = pipeline.run(pil_images, meta)
        logger.info(f"Pipeline completed in {time.time() - t0:.2f}s for {len(pil_images)} image(s)")
        return JSONResponse(content=result)
    except Exception as exc:
        logger.error(f"Pipeline error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Pipeline error: {str(exc)}")