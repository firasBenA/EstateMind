// frontend-client/src/lib/qwenService.ts

export interface QwenDescriptionRequest {
  metadata: {
    property_type: string;
    transaction: string;
    city: string;
    // 🔹 FIX: Allow string OR number (form inputs are strings)
    surface_m2?: string | number;
    rooms?: string | number;
    price?: string | number;
  };
  images: File[];
  language?: 'fr' | 'en' | 'ar';
}

// 🔹 Match your ACTUAL backend response structure
export interface QwenRawResponse {
  bullets: string[];              // ["• Item 1", "• Item 2", ...]
  points_forts?: string;          // "Feature 1, Feature 2, Feature 3"
  ton?: string;                   // "professional"
  description?: string;           // fallback if present
  [key: string]: any;             // allow other fields
}

export interface QwenDescriptionResponse {
  description: string;            // Full description text
  highlights: string[];           // Top 3 highlights
  key_features: string[];         // Key features array
  model_used: string;
  processing_time_seconds: number;
}

export const QWEN_API_BASE = 'http://localhost:8001';

export const qwenService = {
  async generateDescription(
    request: QwenDescriptionRequest
  ): Promise<QwenDescriptionResponse> {
    const formData = new FormData();
    
    // 🔹 Normalize meta convert strings to numbers for backend
    const normalizedMeta = {
      property_type: request.metadata.property_type,
      transaction: request.metadata.transaction,
      city: request.metadata.city,
      // Convert to number if string, handle empty strings
      surface_m2: request.metadata.surface_m2 != null && request.metadata.surface_m2 !== ''
        ? Number(request.metadata.surface_m2)
        : undefined,
      rooms: request.metadata.rooms != null && request.metadata.rooms !== '' && request.metadata.rooms !== 'Studio'
        ? Number(request.metadata.rooms)
        : undefined,
      price: request.metadata.price != null && request.metadata.price !== ''
        ? Number(request.metadata.price)
        : undefined,
    };
    
    formData.append('metadata', JSON.stringify(normalizedMeta));
    formData.append('language', request.language || 'fr');
    
    request.images.forEach((file) => {
      formData.append(`images`, file, file.name);
    });
    
    const response = await fetch(`${QWEN_API_BASE}/generate-description`, {
      method: 'POST',
      body: formData,
    });
    
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new Error(error.detail || `Qwen API error: ${response.status}`);
    }
    
    // 🔹 Parse raw response from backend
    const raw: QwenRawResponse = await response.json();
    
    console.log('🔍 Qwen raw response:', raw); // Debug log
    
    // 🔹 Transform bullets array into clean description
    const bullets = raw.bullets || [];
    
    // Option A: Keep bullet points (exact terminal output)
    const descriptionWithBullets = bullets.join('\n');
    
    // Option B: Remove bullet points for cleaner text
    const descriptionClean = bullets
      .map(b => b.replace(/^•\s*/, '').trim())
      .filter(b => b)
      .join('\n');
    
    // 🔹 Parse points_forts into key_features array
    const pointsForts = raw.points_forts || '';
    const key_features = pointsForts
      .split(/[,\s]+/) // split by comma or space
      .map(f => f.trim())
      .filter(f => f && f.length > 3); // filter short/empty
    
    // 🔹 Build highlights from bullets (define BEFORE logging)
    const highlights = bullets.slice(0, 3).map(b => b.replace(/^•\s*/, '').trim());
    
    // 🔹 Debug: Log transformed values AFTER variables are defined
    console.log('🔍 Transformed response:', {
      description: descriptionClean.slice(0, 100) + '...',
      highlights,
      key_features: key_features.slice(0, 5),
    });
    
    // 🔹 Return transformed response matching frontend expectations
    return {
      // 🔹 Use clean description (or with bullets if you prefer)
      description: descriptionClean || raw.description || 'Description générée par IA',
      
      // 🔹 Top 3 bullets as highlights
      highlights,
      
      // 🔹 Parsed key features
      key_features: key_features.slice(0, 5),
      
      model_used: 'qwen2-vl-2b-instruct',
      processing_time_seconds: 0, // Add if backend sends this
    };
  },
  
  async isHealthy(timeoutMs: number = 3000): Promise<boolean> {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
      
      const response = await fetch(`${QWEN_API_BASE}/health`, {
        method: 'GET',
        signal: controller.signal,
      });
      
      clearTimeout(timeoutId);
      return response.ok;
    } catch {
      return false;
    }
  },
};