# estate_generator.py
import os
import json
import re
import torch
from PIL import Image
from pathlib import Path

# Deep Learning Imports
from ultralytics import YOLO
from transformers import CLIPProcessor, CLIPModel, Qwen2VLForConditionalGeneration, AutoProcessor

# ==========================================
# 1. CONFIGURATION & MAPPINGS
# ==========================================
YOLO_TO_FEATURE = {
    'couch': 'canapé', 'sofa': 'canapé', 'tv': 'télévision',
    'bed': 'lit', 'chair': 'chaise', 'dining table': 'salle à manger',
    'oven': 'cuisine équipée', 'refrigerator': 'réfrigérateur',
    'sink': 'évier', 'toilet': 'salle de bain', 'microwave': 'micro-ondes',
    'potted plant': 'décoration végétale', 'car': 'parking disponible'
}

STYLE_PROMPTS = [
    "modern interior design", "classic traditional style",
    "luxury high-end apartment", "simple budget apartment",
    "bright spacious room", "renovated new condition",
    "cozy intimate room", "open plan living space"
]

# ==========================================
# 2. MODEL LOADING
# ==========================================
def load_models():
    print("📦 Loading models (first run downloads ~2GB)...")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"🖥️  Using device: {device.upper()}")

    yolo = YOLO('yolov8n.pt')
    clip_model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
    clip_processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

    VLM_NAME = "microsoft/Phi-3-mini-4k-instruct"
    qwen_processor = AutoProcessor.from_pretrained(VLM_NAME, trust_remote_code=True)
    qwen_model = Qwen2VLForConditionalGeneration.from_pretrained(
        VLM_NAME,
        torch_dtype=torch.float16 if device == "cuda" else torch.float32,
        device_map="auto" if device == "cuda" else None,
        trust_remote_code=True
    )
    if device == "cpu":
        qwen_model = qwen_model.to(device)

    print("✅ All models loaded successfully!\n")
    return yolo, clip_model, clip_processor, qwen_processor, qwen_model, device

# ==========================================
# 3. CV ANALYSIS
# ==========================================
def run_yolo(img, yolo_model):
    img_rgb = img.convert("RGB")
    results = yolo_model(img_rgb, verbose=False)
    features = set()
    for box in results[0].boxes:
        label = results[0].names[int(box.cls[0])]
        if label in YOLO_TO_FEATURE:
            features.add(YOLO_TO_FEATURE[label])
    return list(features)

def run_clip(img, clip_model, clip_processor, device):
    img_rgb = img.convert("RGB")
    inputs = clip_processor(text=STYLE_PROMPTS, images=img_rgb, return_tensors="pt", padding=True).to(device)
    with torch.no_grad():
        probs = clip_model(**inputs).logits_per_image.softmax(dim=1)[0]
    top = probs.argsort(descending=True)[:3]
    return [STYLE_PROMPTS[i] for i in top]

def analyze_images(images, yolo_model, clip_model, clip_processor, device):
    all_features, all_styles = [], []
    for img in images[:3]:
        print(f"   🔍 Analyzing image...")
        all_features.extend(run_yolo(img, yolo_model))
        all_styles.extend(run_clip(img, clip_model, clip_processor, device))
    return {
        "features": list(set(all_features)),
        "styles": list(set(all_styles))[:3]
    }

# ==========================================
# 4. GENERATION PIPELINE
# ==========================================
def generate_description(images, metadata, yolo_model, clip_model, clip_processor, qwen_processor, qwen_model, device):
    print("🔍 Running YOLO + CLIP analysis...")
    cv = analyze_images(images, yolo_model, clip_model, clip_processor, device)
    print(f"   📦 Detected objects: {cv['features']}")
    print(f"   🎨 Detected styles:  {cv['styles']}")

    meta_str = "; ".join(f"{k}: {v}" for k, v in metadata.items() if v)

    content = []
    for img in images[:3]:
        content.append({"type": "image", "image": img.convert("RGB")})

    prompt_text = f"""
Tu es un expert immobilier tunisien. Génère une description professionnelle en français.

MÉTADONNÉES: {meta_str}

ANALYSE VISUELLE AUTOMATIQUE:
- Objets détectés: {', '.join(cv['features']) if cv['features'] else 'voir images'}
- Style ambiance: {', '.join(cv['styles']) if cv['styles'] else 'voir images'}

INSTRUCTIONS:
1. Génère EXACTEMENT 5 puces en français, concises et professionnelles
2. Utilise le vocabulaire tunisien (S+1, S+2, standing, etc.)
3. Combine ce que tu vois dans les images + les métadonnées
4. Sois factuel et accrocheur

Réponds UNIQUEMENT avec ce JSON (rien d'autre):
{{
  "bullets": ["• point 1", "• point 2", "• point 3", "• point 4", "• point 5"],
  "highlights": ["feature1", "feature2"],
  "tone": "professional"
}}
"""
    content.append({"type": "text", "text": prompt_text})
    messages = [{"role": "user", "content": content}]

    print("🤖 Sending to Qwen2-VL...")
    text_input = qwen_processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    image_inputs = [img.convert("RGB") for img in images[:3]]

    inputs = qwen_processor(
        text=[text_input],
        images=image_inputs,
        return_tensors="pt",
        padding=True
    ).to(qwen_model.device)

    with torch.no_grad():
        output_ids = qwen_model.generate(
            **inputs,
            max_new_tokens=200,
            temperature=0.3,
            do_sample=True,
            pad_token_id=qwen_processor.tokenizer.eos_token_id
        )

    generated_ids = [out[len(inp):] for inp, out in zip(inputs.input_ids, output_ids)]
    raw = qwen_processor.batch_decode(generated_ids, skip_special_tokens=True)[0]

    try:
        match = re.search(r'\{[\s\S]*\}', raw)
        result = json.loads(match.group()) if match else {}
    except Exception:
        result = {}

    if not result.get("bullets"):
        lines = [l.strip() for l in raw.split('\n') if l.strip().startswith('•')]
        result = {
            "bullets": lines[:5] or ["• Description générée automatiquement"],
            "highlights": cv["features"][:3],
            "tone": "professional"
        }

    return result, cv

# ==========================================
# 5. CLI INTERFACE
# ==========================================
def main():
    print("="*60)
    print("  🏠 ESTATEMIND — Générateur de descriptions immobilières")
    print("="*60)

    yolo, clip_model, clip_processor, qwen_processor, qwen_model, device = load_models()

    print("\n📸 Entrez le chemin des images (max 3, séparés par espace):")
    print("💡 Ex: ./photos/living.jpg ./photos/bedroom.png")
    paths = input("> ").strip().split()
    images = []
    for p in paths[:3]:
        if Path(p).exists():
            images.append(Image.open(p))
            print(f"   ✅ Chargé: {p}")
        else:
            print(f"   ❌ Non trouvé: {p}")

    if not images:
        print("⚠️ Aucune image valide. Vérifiez les chemins et relancez.")
        return

    print("\n📝 Informations sur le bien (laissez vide pour ignorer):")
    metadata = {
        "Type":        input("  Type (Appartement/Villa/Studio): "),
        "Transaction": input("  Transaction (Location/Vente): "),
        "Ville":       input("  Ville: "),
        "Superficie":  input("  Superficie (m²): "),
        "Pièces":      input("  Pièces (ex: S+2): "),
        "Salles bain": input("  Salles de bain: "),
        "Prix":        input("  Prix (TND): "),
        "Meublé":      input("  Meublé (Oui/Non): "),
    }
    metadata = {k: v for k, v in metadata.items() if v.strip()}

    print("\n" + "="*60)
    result, cv = generate_description(images, metadata, yolo, clip_model, clip_processor, qwen_processor, qwen_model, device)

    print("\n✅ DESCRIPTION GÉNÉRÉE")
    print("-"*60)
    for bullet in result.get("bullets", []):
        print(bullet)
    print("-"*60)
    print(f"Points forts: {', '.join(result.get('highlights', []))}")
    print(f"Ton: {result.get('tone', 'professional')}")
    print("="*60)

if __name__ == "__main__":
    main()