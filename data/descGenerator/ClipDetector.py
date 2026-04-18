"""
CLIPDetector: uses CLIP to classify property images into style/quality/condition categories.
Returns soft labels used by the Phi-3 prompt builder.
"""

import logging
import torch
from PIL import Image

logger = logging.getLogger("clip_detector")

# ── Label banks for zero-shot classification ──────────────────────────────────

STYLE_LABELS = [
    "modern minimalist interior",
    "traditional arabic style interior",
    "luxury high-end interior",
    "rustic countryside style",
    "industrial loft style",
    "coastal mediterranean style",
    "contemporary open plan",
    "classic european style",
]

CONDITION_LABELS = [
    "newly renovated, excellent condition",
    "well maintained, good condition",
    "needs renovation, older condition",
    "under construction or unfinished",
]

LIGHTING_LABELS = [
    "bright, natural sunlight",
    "well lit artificial lighting",
    "dim or dark interior",
    "outdoor with natural light",
]

SPACE_LABELS = [
    "spacious open area",
    "compact cozy space",
    "large room with high ceilings",
    "small but well organized",
]


class CLIPDetector:
    def __init__(self, clip_model, clip_processor, device: str):
        self.model = clip_model
        self.processor = clip_processor
        self.device = device

    def detect(self, images: list[Image.Image]) -> dict:
        """
        Classify images across style/condition/lighting/space dimensions.

        Returns:
          {
            "style": "modern minimalist interior",
            "condition": "newly renovated, excellent condition",
            "lighting": "bright, natural sunlight",
            "space_feel": "spacious open area",
            "style_confidence": 0.74
          }
        """
        if self.model is None:
            return self._empty()

        try:
            results = {}
            results.update(self._classify(images, STYLE_LABELS, "style"))
            results.update(self._classify(images, CONDITION_LABELS, "condition"))
            results.update(self._classify(images, LIGHTING_LABELS, "lighting"))
            results.update(self._classify(images, SPACE_LABELS, "space_feel"))
            return results
        except Exception as exc:
            logger.warning(f"CLIP detection failed: {exc}")
            return self._empty()

    def _classify(self, images: list[Image.Image], labels: list[str], key: str) -> dict:
        """
        Average logits across all images, pick top label.
        """
        probs_list = []
        for img in images:
            inputs = self.processor(
                text=labels,
                images=img,
                return_tensors="pt",
                padding=True,
            ).to(self.device)
            with torch.no_grad():
                outputs = self.model(**inputs)
                logits = outputs.logits_per_image  # (1, num_labels)
                probs = logits.softmax(dim=1).cpu().squeeze(0)
                probs_list.append(probs)

        avg_probs = torch.stack(probs_list).mean(dim=0)
        top_idx = int(avg_probs.argmax())
        top_conf = float(avg_probs[top_idx])

        result = {key: labels[top_idx]}
        if key == "style":
            result["style_confidence"] = round(top_conf, 3)
        return result

    @staticmethod
    def _empty() -> dict:
        return {
            "style": "modern interior",
            "condition": "good condition",
            "lighting": "well lit",
            "space_feel": "comfortable space",
            "style_confidence": 0.0,
        }