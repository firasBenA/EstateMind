# data/descGenerator/llm_generator.py

import json
import logging
import re
import torch
from PIL import Image

logger = logging.getLogger("llm_generator")


class LLMGenerator:
    def __init__(self, vl_model, vl_processor, device: str):
        self.model = vl_model
        self.processor = vl_processor
        self.device = device

    # ✅ FIX: Change 'meta' to 'metadata' to match pipeline.py call
    def generate(self, images: list, metadata: dict) -> dict:
        """Generate description using Qwen2-VL with actual images."""
        tone = metadata.get("tone", "professional")
        
        # Resize images to save memory
        pil_images = [self._resize_image(img.convert("RGB")) for img in images]
        
        # Build the French prompt with Tunisian context
        prompt_text = self._build_vl_prompt(metadata, tone)
        
        # Build conversation format for Qwen2-VL
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image", "image": img} for img in pil_images
                ] + [
                    {"type": "text", "text": prompt_text}
                ]
            }
        ]
        
        # Apply chat template
        text_input = self.processor.apply_chat_template(
            messages, 
            tokenize=False, 
            add_generation_prompt=True
        )
        
        # Process inputs
        inputs = self.processor(
            text=[text_input],
            images=pil_images,
            return_tensors="pt",
            padding=True
        ).to(self.device)
        
        # Generate
        with torch.no_grad():
            output_ids = self.model.generate(
                **inputs,
                max_new_tokens=600,
                temperature=0.3,
                do_sample=True,
                pad_token_id=self.processor.tokenizer.eos_token_id
            )
        
        # Decode
        generated_ids = [out[len(inp):] for inp, out in zip(inputs.input_ids, output_ids)]
        raw_text = self.processor.batch_decode(generated_ids, skip_special_tokens=True)[0].strip()
        
        logger.info(f"🤖 VLM Raw Output: {raw_text[:200]}...")
        
        return self._parse_output(raw_text, tone)

    def _resize_image(self, img, max_size: int = 512):
        """Resize image to save memory."""
        img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
        return img

    # In data/descGenerator/llm_generator.py

    def _build_vl_prompt(self, meta: dict, tone: str) -> str:
        """Build detailed French prompt with Tunisian real estate vocabulary."""
        
        # Format metadata
        meta_lines = []
        if meta.get("type"): meta_lines.append(f"Type: {meta['type']}")
        if meta.get("city"): meta_lines.append(f"Ville: {meta['city']}")
        if meta.get("transaction"): 
            trans = "Vente" if meta["transaction"] == "sale" else "Location"
            meta_lines.append(f"Transaction: {trans}")
        if meta.get("surface"): meta_lines.append(f"Surface: {meta['surface']} m²")
        if meta.get("rooms"): meta_lines.append(f"Pièces: S+{meta['rooms']}")
        if meta.get("price"): meta_lines.append(f"Prix: {meta['price']} TND")
        
        meta_str = " | ".join(meta_lines)
        
        # ✅ MORE DETAILED PROMPT - Forces image analysis
        return f"""Tu es un expert immobilier tunisien. Ton rôle est d'analyser EN DÉTAIL les images fournies et de rédiger une description complète et professionnelle.

    INFORMATIONS SUR LE BIEN:
    {meta_str}

    INSTRUCTIONS DÉTAILLÉES:
    1. **Analyse visuelle approfondie**: Décris précisément ce que tu vois dans CHAQUE image (pièces, équipements, état, luminosité, décoration, meubles, électroménager, etc.)
    2. **Combine avec les métadonnées**: Intègre les informations techniques (surface, prix, ville) naturellement dans la description
    3. **Utilise le vocabulaire tunisien**: "S+1", "S+2", "standing", "bien ensoleillé", "vue dégagée", "proche commodités", "cuisine américaine équipée", "dégagement", "débarras", etc.
    4. **Sois descriptif et vendeur**: Mentionne les points forts visibles (ex: "cuisine équipée avec réfrigérateur et évier", "salon spacieux avec canapé", "parking disponible", "décoration végétale", etc.)
    5. **Génère 5 puces détaillées** en français, chacune décrivant un aspect différent du bien

    FORMAT DE RÉPONSE EXIGÉ (JSON strict):
    {{
    "bullets": [
        "• [Type de bien] de [surface] m² à [transaction] à [ville] - [détail supplémentaire]",
        "• [Description d'une pièce ou équipement visible dans les images]",
        "• [Autre caractéristique visible: meubles, électroménager, décoration, etc.]",
        "• [Informations sur le prix ou la localisation]",
        "• [État du bien ou autres avantages visibles]"
    ],
    "highlights": [
        "[Équipement ou caractéristique principale 1]",
        "[Équipement ou caractéristique principale 2]",
        "[Équipement ou caractéristique principale 3]"
    ],
    "tone": "{tone}"
    }}

    EXEMPLE DE CE QUE JE VEUX:
    {{
    "bullets": [
        "• Une villa de 100 m² à vendre à Tunis",
        "• Située dans un quartier résidentiel prestigieux",
        "• Comprend une cuisine équipée avec un canapé, un évier, une chaise, un parking et une décoration végétale",
        "• Le prix de la villa est de 120 000 dinars tunisiens",
        "• Le bien est meublé"
    ],
    "highlights": ["Villa moderne et luxueuse", "Cuisine équipée complète", "Parking disponible"],
    "tone": "professional"
    }}

    Réponds UNIQUEMENT avec l'objet JSON ci-dessus. Pas de texte avant, pas de texte après."""

        # In data/descGenerator/llm_generator.py

    def _parse_output(self, raw: str, tone: str) -> dict:
        """Parse JSON and format output beautifully."""
        text = re.sub(r"```(?:json)?", "", raw).strip()
        
        try:
            result = json.loads(text)
            result = self._validate_schema(result, tone)
            
            # ✅ LOG THE FORMATTED OUTPUT (like Kaggle)
            logger.info("\n" + "="*60)
            logger.info("✅ DESCRIPTION GÉNÉRÉE")
            logger.info("="*60)
            for bullet in result.get("bullets", []):
                logger.info(bullet)
            logger.info("-"*60)
            logger.info(f"Points forts: {', '.join(result.get('highlights', []))}")
            logger.info(f"Ton: {result.get('tone', 'professional')}")
            logger.info("="*60 + "\n")
            
            return result
            
        except json.JSONDecodeError:
            pass
        
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            try:
                result = json.loads(match.group())
                result = self._validate_schema(result, tone)
                
                # Log formatted output
                logger.info("\n" + "="*60)
                logger.info("✅ DESCRIPTION GÉNÉRÉE")
                logger.info("="*60)
                for bullet in result.get("bullets", []):
                    logger.info(bullet)
                logger.info("-"*60)
                logger.info(f"Points forts: {', '.join(result.get('highlights', []))}")
                logger.info("="*60 + "\n")
                
                return result
            except json.JSONDecodeError:
                pass
        
        logger.warning(f"Could not parse JSON from VLM, using fallback.")
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
        lines = [l.strip() for l in raw.split('\n') if l.strip().startswith(('•', '-', '*'))]
        if not lines:
            lines = [l.strip() for l in raw.split('\n') if len(l.strip()) > 20]
        return {
            "bullets": lines[:5] if lines else ["• Description générée automatiquement"],
            "highlights": ["Immobilier", "Tunisie", "Qualité"],
            "tone": tone,
        }