# data/generator/vl_generator.py

import json
import re
import torch
from PIL import Image

def generate_description_vl(model, processor, images: list[Image.Image], metadata: dict, device: str):
    """
    Generates a description using Qwen2-VL by looking at the images directly.
    """
    
    # Prepare Metadata String
    meta_str = "; ".join(f"{k}: {v}" for k, v in metadata.items() if v)

    # Build the Prompt for Qwen2-VL
    # We ask it to act as a Tunisian real estate expert
    system_prompt = f"""
Tu es un expert immobilier tunisien. Génère une description professionnelle en français pour une annonce immobilière.

MÉTADONNÉES DU BIEN:
{meta_str}

INSTRUCTIONS:
1. Analyse les images fournies pour décrire l'état, le style et les équipements visibles.
2. Combine cette analyse visuelle avec les métadonnées.
3. Utilise un vocabulaire professionnel (ex: standing, lumineux, spacieux).
4. Réponds UNIQUEMENT avec un objet JSON valide contenant:
   - "bullets": Une liste de 5 points forts concis.
   - "highlights": Une liste de 3 mots-clés principaux.
   - "tone": "professional"

EXEMPLE DE FORMAT JSON:
{{
  "bullets": [
    "• Villa moderne de 100m² située à Tunis",
    "• Cuisine entièrement équipée avec électroménager haut de gamme",
    "• Grand salon lumineux donnant sur une terrasse privée",
    "• Proche des commodités et des axes principaux",
    "• Finitions de standing et état impeccable"
  ],
  "highlights": ["Standing", "Lumineux", "Bien équipé"],
  "tone": "professional"
}}
"""

    # Prepare Messages for Qwen2-VL
    # Qwen2-VL expects images in the content list
    content = []
    
    # Add up to 3 images to avoid memory issues
    for img in images[:3]:
        content.append({
            "type": "image", 
            "image": img.convert("RGB")
        })
        
    content.append({
        "type": "text", 
        "text": system_prompt
    })

    messages = [{"role": "user", "content": content}]

    # Apply Chat Template
    text_input = processor.apply_chat_template(
        messages, 
        tokenize=False, 
        add_generation_prompt=True
    )
    
    # Process Inputs
    image_inputs = [img.convert("RGB") for img in images[:3]]
    
    inputs = processor(
        text=[text_input],
        images=image_inputs,
        return_tensors="pt",
        padding=True
    ).to(device)

    # Generate
    with torch.no_grad():
        output_ids = model.generate(
            **inputs,
            max_new_tokens=512,
            temperature=0.2, # Lower temp for more factual output
            do_sample=True,
            pad_token_id=processor.tokenizer.eos_token_id
        )

    # Decode Output
    generated_ids = [
        out[len(inp):]
        for inp, out in zip(inputs.input_ids, output_ids)
    ]
    raw_text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
    
    # Parse JSON
    try:
        match = re.search(r'\{[\s\S]*\}', raw_text)
        if match:
            result = json.loads(match.group())
        else:
            raise ValueError("No JSON found")
    except Exception as e:
        print(f"JSON Parse Error: {e}. Raw: {raw_text[:200]}")
        # Fallback
        result = {
            "bullets": ["• Description générée automatiquement par IA.", "• Voir images pour plus de détails."],
            "highlights": ["Immobilier", "Tunisie"],
            "tone": "professional"
        }

    return result