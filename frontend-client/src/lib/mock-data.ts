export interface Listing {
  id: string;
  source_name: string;
  url: string;
  title: string;
  description: string;
  price: number;
  currency: string;
  transaction_type: "sale" | "rent";
  type: "apartment" | "house" | "land" | "commercial";
  rooms: number;
  city: string;
  municipality: string;
  zone: string;
  region: string;
  surface: number;
  features: string[];
  poi: string[];
  images: { url: string; label: string }[];
  images_count: number;
  price_per_m2: number;
  room_image_ratio: number;
  fraud_score: number;
  //fraud_flag: boolean;
  fraud_reason: string | null;
  fraud_model_used: string | null;
  flagged_at: string | null;
  reliability_score: number;
  reliability_level: "HIGH" | "MEDIUM" | "LOW";
  is_outlier: boolean;
  outlier_flags: string[];
  suspected_duplicate: boolean;
  scraped_at: string;
  last_updated: string;
  latitude: number;
  longitude: number;
  change_type: string;
  has_price_history: boolean;
  price_delta: number | null;
  price_delta_pct: number | null;
  should_drop: boolean;
  normalized: boolean;
  nlp_enriched: boolean;
}

const cities = ["Tunis", "Sfax", "Sousse", "Bizerte", "Nabeul", "Ariana", "La Marsa", "Hammamet", "Monastir", "Gabès", "Kairouan", "Carthage"];
const zones = ["Centre Ville", "Zone Industrielle", "Banlieue Nord", "Banlieue Sud", "Zone Touristique", "Quartier Résidentiel"];
const sources = ["tayara", "mubawab", "affare"];
const types: Listing["type"][] = ["apartment", "house", "land", "commercial"];
const transTypes: Listing["transaction_type"][] = ["sale", "rent"];
const features = ["Parking", "Balcony", "Garden", "Pool", "Elevator", "Security", "Furnished", "Air Conditioning", "Sea View", "Terrace"];
const imageLabels = ["room", "kitchen", "bathroom", "garden", "exterior", "living room"];

function rand(min: number, max: number) { return Math.floor(Math.random() * (max - min + 1)) + min; }
function pick<T>(arr: T[]): T { return arr[rand(0, arr.length - 1)]; }
function pickN<T>(arr: T[], n: number): T[] {
  const shuffled = [...arr].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, n);
}

function generateListing(i: number): Listing {
  const type = pick(types);
  const transType = pick(transTypes);
  const city = pick(cities);
  const surface = type === "land" ? rand(200, 2000) : rand(40, 300);
  const basePrice = transType === "sale"
    ? (type === "land" ? rand(50000, 500000) : rand(100000, 2000000))
    : (type === "land" ? rand(500, 3000) : rand(400, 5000));
  const price = Math.round(basePrice / 1000) * 1000;
  const rooms = type === "land" ? 0 : rand(1, 6);
  const pricePerM2 = surface > 0 ? Math.round(price / surface) : 0;
  const fraudScore = Math.random();
  const fraudFlag = fraudScore > 0.75;
  const reliabilityScore = Math.round((1 - fraudScore * 0.5) * 100);
  const daysAgo = rand(0, 60);
  const scrapedAt = new Date(Date.now() - daysAgo * 86400000).toISOString();
  const imgCount = rand(1, 6);

  return {
    id: `lst-${String(i).padStart(4, "0")}`,
    source_name: pick(sources),
    url: `https://example.com/listing/${i}`,
    title: `${type === "apartment" ? "Appartement" : type === "house" ? "Villa" : type === "land" ? "Terrain" : "Local Commercial"} ${rooms > 0 ? `S+${rooms}` : ""} à ${city}`,
    description: `Belle propriété située à ${city}. ${pickN(features, 3).join(", ")}. Proche de toutes commodités. Surface de ${surface}m². Idéal pour ${transType === "rent" ? "location" : "investissement"}.`,
    price,
    currency: "TND",
    transaction_type: transType,
    type,
    rooms,
    city,
    municipality: city,
    zone: pick(zones),
    region: city,
    surface,
    features: pickN(features, rand(2, 5)),
    poi: ["École", "Supermarché", "Transport"],
    images: Array.from({ length: imgCount }, (_, j) => ({
      url: `https://images.unsplash.com/photo-1560448204e02f11c3d0e2?w=600&h=400&fit=crop`,
      label: pick(imageLabels),
    })),
    images_count: imgCount,
    price_per_m2: pricePerM2,
    room_image_ratio: rooms > 0 ? imgCount / rooms : 0,
    fraud_score: Math.round(fraudScore * 100) / 100,
    //fraud_flag: fraudFlag,
    fraud_reason: fraudFlag ? pick(["Suspicious pricing", "Duplicate images detected", "Inconsistent room count", "Price too low for area"]) : null,
    fraud_model_used: fraudFlag ? "ensemble_v2" : null,
    flagged_at: fraudFlag ? scrapedAt : null,
    reliability_score: reliabilityScore,
    reliability_level: reliabilityScore >= 70 ? "HIGH" : reliabilityScore >= 40 ? "MEDIUM" : "LOW",
    is_outlier: Math.random() > 0.85,
    outlier_flags: Math.random() > 0.85 ? pickN(["price_too_low", "price_too_high", "surface_mismatch"], rand(1, 2)) : [],
    suspected_duplicate: Math.random() > 0.9,
    scraped_at: scrapedAt,
    last_updated: scrapedAt,
    latitude: 33.8 + Math.random() * 3,
    longitude: 8.5 + Math.random() * 2.5,
    change_type: pick(["new", "updated", "unchanged"]),
    has_price_history: Math.random() > 0.6,
    price_delta: Math.random() > 0.5 ? rand(-50000, 50000) : null,
    price_delta_pct: null,
    should_drop: false,
    normalized: Math.random() > 0.2,
    nlp_enriched: Math.random() > 0.3,
  };
}

export const mockListings: Listing[] = Array.from({ length: 200 }, (_, i) => generateListing(i + 1));

export const CITIES = [...new Set(mockListings.map(l => l.city))].sort();
export const REGIONS = [...new Set(mockListings.map(l => l.region))].sort();

// Unsplash real estate images
const realImages = [
  "https://images.unsplash.com/photo-1560448204-e02f11c3d0e2?w=600&h=400&fit=crop",
  "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=600&h=400&fit=crop",
  "https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=600&h=400&fit=crop",
  "https://images.unsplash.com/photo-1512917774080-9991f1c4c750?w=600&h=400&fit=crop",
  "https://images.unsplash.com/photo-1570129477492-45c003edd2be?w=600&h=400&fit=crop",
  "https://images.unsplash.com/photo-1554995207-c18c203602cb?w=600&h=400&fit=crop",
  "https://images.unsplash.com/photo-1600607687939-ce8a6c25118c?w=600&h=400&fit=crop",
  "https://images.unsplash.com/photo-1600566753376-12c8ab7fb75b?w=600&h=400&fit=crop",
];

// Assign real images to listings
mockListings.forEach((l, i) => {
  l.images = l.images.map((img, j) => ({
    ...img,
    url: realImages[(i + j) % realImages.length],
  }));
});

export function formatPrice(price: number): string {
  return price.toLocaleString("fr-TN") + " TND";
}

export function formatPricePerM2(price: number): string {
  return price.toLocaleString("fr-TN") + " TND/m²";
}

export function formatDate(dateStr: string): string {
  const d = new Date(dateStr);
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return `${d.getDate()} ${months[d.getMonth()]} ${d.getFullYear()} at ${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

export function getStats() {
  //const nonFlagged = mockListings.filter(l => !l.fraud_flag);
  const weekAgo = Date.now() - 7 * 86400000;
  return {
    totalListings: mockListings.length,
    citiesCovered: CITIES.length,
    avgPricePerM2: Math.round(mockListings.reduce((s, l) => s + l.price_per_m2, 0) / mockListings.length),
    listingsThisWeek: mockListings.filter(l => new Date(l.scraped_at).getTime() > weekAgo).length,
  };
}
