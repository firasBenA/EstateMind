"""
YOLOExtractor: runs YOLOv8 on property images and returns structured room/object features.
Designed to be called per-image and results merged across multiple images.
"""

import logging
from collections import Counter
from PIL import Image
import numpy as np

logger = logging.getLogger("yolo_extractor")

# COCO class names relevant to real estate
REAL_ESTATE_CLASSES = {
    # Furniture / fixtures
    "couch", "chair", "bed", "dining table", "toilet", "sink",
    "refrigerator", "oven", "microwave", "tv",
    # Structural
    "door", "window",
    # Outdoor
    "potted plant", "umbrella", "car",
    # People (count presence)
    "person",
}

# Mapping COCO labels → real estate feature language
LABEL_TO_FEATURE = {
    "couch": "furnished living room",
    "chair": "seating area",
    "bed": "furnished bedroom",
    "dining table": "dining space",
    "toilet": "bathroom",
    "sink": "bathroom/kitchen",
    "refrigerator": "equipped kitchen",
    "oven": "equipped kitchen",
    "microwave": "equipped kitchen",
    "tv": "entertainment space",
    "potted plant": "interior greenery",
    "car": "parking available",
}


class YOLOExtractor:
    def __init__(self, yolo_model):
        self.model = yolo_model

    def extract(self, images: list[Image.Image]) -> dict:
        """
        Run YOLO on all images and aggregate detected features.

        Returns:
          {
            "detected_objects": ["bed", "couch", ...],
            "feature_tags": ["furnished bedroom", "equipped kitchen", ...],
            "room_hints": ["bedroom", "kitchen", "living room"],
            "confidence": 0.82
          }
        """
        if self.model is None:
            logger.debug("YOLO not available — returning empty features")
            return {"detected_objects": [], "feature_tags": [], "room_hints": [], "confidence": 0.0}

        all_labels: list[str] = []
        total_conf: list[float] = []

        for img in images:
            arr = np.array(img)
            try:
                results = self.model(arr, verbose=False, conf=0.35)
                for r in results:
                    for box in r.boxes:
                        cls_id = int(box.cls[0])
                        label = r.names[cls_id].lower()
                        conf = float(box.conf[0])
                        all_labels.append(label)
                        total_conf.append(conf)
            except Exception as exc:
                logger.warning(f"YOLO inference error on image: {exc}")

        if not all_labels:
            return {"detected_objects": [], "feature_tags": [], "room_hints": [], "confidence": 0.0}

        # Deduplicate by most common
        counts = Counter(all_labels)
        detected = [label for label, _ in counts.most_common(15)]
        feature_set: set[str] = set()
        for label in detected:
            if label in LABEL_TO_FEATURE:
                feature_set.add(LABEL_TO_FEATURE[label])

        # Infer room types
        room_hints = _infer_rooms(counts)
        avg_conf = sum(total_conf) / len(total_conf) if total_conf else 0.0

        return {
            "detected_objects": detected,
            "feature_tags": list(feature_set),
            "room_hints": room_hints,
            "confidence": round(avg_conf, 3),
        }


def _infer_rooms(counts: Counter) -> list[str]:
    rooms = []
    if counts.get("bed", 0) > 0:
        rooms.append("bedroom")
    if counts.get("toilet", 0) > 0 or counts.get("sink", 0) > 0:
        rooms.append("bathroom")
    if counts.get("couch", 0) > 0 or counts.get("tv", 0) > 0:
        rooms.append("living room")
    if counts.get("refrigerator", 0) > 0 or counts.get("oven", 0) > 0:
        rooms.append("kitchen")
    if counts.get("dining table", 0) > 0:
        rooms.append("dining room")
    return rooms