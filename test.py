# test_llm.py
import json
import requests

API_KEY = "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"
MODEL = "mistralai/mistral-small-2603"  # Best for extraction

def test_extraction():
    """Test the LLM extractor"""
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    test_text = """
    Bel appartement S+3 situé à La Marsa, superficie 145m², vue sur mer, 
    avec piscine et parking. Prix: 850 000 TND. 4 chambres, salon, cuisine équipée.
    """
    
    prompt = f"""Extract structured data from this Tunisian real estate listing.

Listing text: {test_text}

Return ONLY a JSON object with these exact fields:
{{
  "rooms": number or null,
  "surface": number or null,
  "price": number or null,
  "city": string or null,
  "governorate": string or null,
  "district": string or null,
  "transaction_type": "Sale" or "Rent" or null,
  "property_type": "Apartment" or "Villa" or "Land" or "Commercial" or "Other" or null,
  "features": ["feature1", "feature2"] or null
}}

JSON only, no other text:"""
    
    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 500
        }
    )
    
    if response.status_code == 200:
        result = response.json()
        content = result['choices'][0]['message']['content']
        
        # Clean and parse
        content = content.strip()
        if content.startswith('```json'):
            content = content[7:]
        if content.startswith('```'):
            content = content[3:]
        if content.endswith('```'):
            content = content[:-3]
        content = content.strip()
        
        try:
            extracted = json.loads(content)
            print("Extracted data:")
            print(json.dumps(extracted, indent=2, ensure_ascii=False))
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
            print(f"Raw content: {content}")
    else:
        print(f"Error: {response.status_code}")
        print(response.text)

def test_batch_extraction():
    """Test batch extraction with multiple listings"""
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    listings = [
        "Appartement 3 pièces à Tunis, 120m², 450000 TND, parking inclus",
        "Villa avec piscine à Hammamet, 280m², terrain 1000m², 1.2M TND, vue mer",
        "Studio meublé à louer Lac 2, 45m², loyer 1200 DT/mois, proche commerces",
        "Terrain à bâtir 500m² à Nabeul, zone résidentielle, 150000 TND",
        "Bureau commercial 80m² à Sfax centre, 250000 TND, rénové"
    ]
    
    listings_text = []
    for idx, listing in enumerate(listings):
        listings_text.append(f"Listing {idx+1}: {listing}")
    
    combined = "\n---\n".join(listings_text)
    
    prompt = f"""Extract structured data from these {len(listings)} Tunisian real estate listings.

{combined}

Return a JSON array with one object per listing. Each object should have:
{{
  "rooms": number or null,
  "surface": number or null,
  "price": number or null,
  "city": string or null,
  "governorate": string or null,
  "transaction_type": "Sale" or "Rent" or null,
  "property_type": "Apartment" or "Villa" or "Land" or "Commercial" or "Other" or null,
  "features": ["feature1", "feature2"] or null
}}

JSON array only, no other text:"""
    
    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 2000
        }
    )
    
    if response.status_code == 200:
        result = response.json()
        content = result['choices'][0]['message']['content']
        
        # Clean response
        content = content.strip()
        if content.startswith('```json'):
            content = content[7:]
        if content.startswith('```'):
            content = content[3:]
        if content.endswith('```'):
            content = content[:-3]
        content = content.strip()
        
        try:
            extracted = json.loads(content)
            print("\nBatch extraction results:")
            for i, item in enumerate(extracted):
                print(f"\nListing {i+1}:")
                print(json.dumps(item, indent=2, ensure_ascii=False))
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
            print(f"Raw content: {content}")
    else:
        print(f"Error: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    print("Testing single extraction...")
    test_extraction()
    
    print("\n" + "="*50)
    print("Testing batch extraction...")
    test_batch_extraction()