# data/generator/llm_generator.py

import json
import logging
import re
import torch

logger = logging.getLogger("llm_generator")

GENERATION_CONFIG = dict(
    max_new_tokens=512,
    temperature=0.7,
    top_p=0.9,
    top_k=50,
    repetition_penalty=1.1,
    do_sample=True,
)


class LLMGenerator:
    def __init__(self, llm_model, llm_tokenizer, device: str):
        self.model = llm_model
        self.tokenizer = llm_tokenizer
        self.device = device

    def generate(self, yolo_features: dict, clip_features: dict, meta: dict) -> dict:
        tone = metadata.get("tone", "professional")
        prompt = self._build_prompt(yolo_features, clip_features, metadata, tone)

        inputs = self.tokenizer(prompt, return_tensors="pt")
        
        # ✅ CRITICAL: Move inputs to the same device as the model
        inputs = inputs.to(self.model.device)
        
        input_len = inputs["input_ids"].shape[1]

        with torch.no_grad():
            output_ids = self.model.generate(
                **inputs,
                pad_token_id=self.tokenizer.eos_token_id,
                **GENERATION_CONFIG,
            )

        new_ids = output_ids[0][input_len:]
        raw_text = self.tokenizer.decode(new_ids, skip_special_tokens=True).strip()

        return self._parse_output(raw_text, tone)

    def _build_prompt(self, yolo: dict, clip: dict, meta: dict, tone: str) -> str:
        prop_type = meta.get("type", "property")
        city = meta.get("city", "")
        price = meta.get("price", "")
        currency = meta.get("currency", "TND")
        surface = meta.get("surface", "")
        rooms = meta.get("rooms", "")
        transaction = meta.get("transaction", "sale")

        room_hints = yolo.get("room_hints", [])
        feature_tags = yolo.get("feature_tags", [])
        style = clip.get("style", "modern interior")
        condition = clip.get("condition", "good condition")
        lighting = clip.get("lighting", "well lit")
        space_feel = clip.get("space_feel", "comfortable space")

        rooms_str = f"{rooms}-room " if rooms else ""
        surface_str = f"{surface} m²" if surface else ""
        price_str = f"{price:,} {currency}" if price else ""

        detected_features = ", ".join(feature_tags) if feature_tags else "various modern amenities"
        detected_rooms = ", ".join(room_hints) if room_hints else "multiple rooms"

        tone_instruction = {
            "professional": "Use clear, factual, professional real estate language.",
            "luxury": "Use elevated, aspirational language emphasizing exclusivity.",
            "casual": "Use friendly, approachable language.",
        }.get(tone, "Use clear, professional real estate language.")

        system = (
            "You are an expert real estate copywriter specializing in Tunisian property listings. "
            f"{tone_instruction} "
            "ALWAYS respond with valid JSON only. No preamble, no explanation."
        )

        user_content = f"""Generate a real estate description based on this analysis.

PROPERTY DETAILS:
- Type: {rooms_str}{prop_type}
- Location: {city}
- Transaction: {transaction}
- Surface: {surface_str}
- Price: {price_str}

AI VISUAL ANALYSIS:
- Detected rooms: {detected_rooms}
- Interior style: {style}
- Condition: {condition}
- Lighting: {lighting}
- Space feel: {space_feel}
- Key features: {detected_features}

OUTPUT FORMAT (strict JSON):
{{
  "bullets": ["Feature 1", "Feature 2", "Feature 3", "Feature 4", "Feature 5"],
  "highlights": ["Highlight 1", "Highlight 2", "Highlight 3"],
  "tone": "{tone}"
}}

Respond ONLY with the JSON object."""

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user_content}
        ]
        
        prompt = self.tokenizer.apply_chat_template(
            messages, 
            tokenize=False, 
            add_generation_prompt=True
        )
        
        return prompt

    def _parse_output(self, raw: str, tone: str) -> dict:
        text = re.sub(r"```(?:json)?", "", raw).strip()
        try:
            result = json.loads(text)
            return self._validate_schema(result, tone)
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            try:
                result = json.loads(match.group())
                return self._validate_schema(result, tone)
            except json.JSONDecodeError:
                pass

        logger.warning(f"Could not parse JSON, using fallback. Raw: {raw[:200]}")
        return self._fallback(raw, tone)

    def _validate_schema(self, result: dict, tone: str) -> dict:
        bullets = result.get("bullets", [])
        highlights = result.get("highlights", [])
        if not isinstance(bullets, list): bullets = [str(bullets)]
        if not isinstance(highlights, list): highlights = [str(highlights)]
        return {
            "bullets": bullets[:8],
            "highlights": highlights[:4],
            "tone": result.get("tone", tone),
        }

    def _fallback(self, raw: str, tone: str) -> dict:
        lines = [l.strip("- •*").strip() for l in raw.split("\n") if len(l.strip()) > 20]
        return {
            "bullets": lines[:5] if lines else ["Property features modern amenities"],
            "highlights": lines[:2] if lines else ["Prime location", "Great value"],
            "tone": tone,
        }