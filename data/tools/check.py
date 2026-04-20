# # tools/show_local_schema.py
# """Show exact local PostgreSQL schema to replicate in Supabase."""

# import os
# import psycopg2
# from dotenv import load_dotenv
# load_dotenv()

# conn = psycopg2.connect(
#     host=os.getenv("POSTGRES_HOST", "localhost"),
#     port=int(os.getenv("POSTGRES_PORT", "5432")),
#     dbname=os.getenv("POSTGRES_DATABASE", "estatemind"),
#     user=os.getenv("POSTGRES_USER", "postgres"),
#     password=os.getenv("POSTGRES_PASSWORD", ""),
# )

# with conn.cursor() as cur:
#     # Get listings columns
#     cur.execute("""
#         SELECT column_name, data_type, is_nullable
#         FROM information_schema.columns
#         WHERE table_name = 'listings'
#         ORDER BY ordinal_position
#     """)
    
#     print("-- LISTINGS TABLE --")
#     for row in cur.fetchall():
#         print(f"{row[0]}: {row[1]} (nullable={row[2]})")
    
#     # Get image_embeddings columns
#     cur.execute("""
#         SELECT column_name, data_type, is_nullable
#         FROM information_schema.columns
#         WHERE table_name = 'image_embeddings'
#         ORDER BY ordinal_position
#     """)
    
#     print("\n-- IMAGE_EMBEDDINGS TABLE --")
#     for row in cur.fetchall():
#         print(f"{row[0]}: {row[1]} (nullable={row[2]})")

# conn.close()


# generate_recommendation_data.py
# tools/check.py - Fixed version without Django dependency
import random
import psycopg2
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

# ============================================
# DATABASE CONNECTION
# ============================================

PG_CONN = dict(
    host="aws-1-eu-central-1.pooler.supabase.com",
    port=5432,
    dbname="postgres",
    user="postgres.amxnojlfczwffvtwutrb",
    password="capTEEMO5000",  # ⚠️ REPLACE WITH YOUR ACTUAL PASSWORD
    sslmode="require"
)

# Your 9 agencies from the query
AGENCIES = [
    "tunisieannonce", "century21", "mubawab", "newkey", 
    "affare", "tecnocasa", "zitouna_immo", "darcom", "verdar"
]

# ============================================
# HELPER FUNCTIONS
# ============================================

def simple_hash_password(password: str) -> str:
    """Simple password hashing without Django"""
    # This creates a Django-compatible PBKDF2 hash
    salt = secrets.token_hex(16)
    hash_obj = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"pbkdf2_sha256$100000${salt}${hash_obj.hex()}"

def get_listings_by_agency():
    """Get listings grouped by agency"""
    listings_by_agency = {}
    
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            for agency in AGENCIES:
                cur.execute("""
                    SELECT id, city, property_type, price, surface, region, source_name
                    FROM listings
                    WHERE source_name = %s AND should_drop IS NOT TRUE
                    LIMIT 500
                """, [agency])
                rows = cur.fetchall()
                listings_by_agency[agency] = rows
                print(f"  - {agency}: {len(rows)} listings")
    
    return listings_by_agency

def create_mock_users(num_users=50):
    """Create mock registered users WITHOUT Django"""
    users = []
    
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            for i in range(num_users):
                username = f"buyer_{i+1}"
                email = f"user_{i+1}@example.com"
                
                # Check if user exists
                cur.execute("SELECT id FROM auth_user WHERE username = %s", [username])
                existing = cur.fetchone()
                
                if not existing:
                    # Create new user with simple hash
                    hashed_password = simple_hash_password("password123")
                    
                    cur.execute("""
                        INSERT INTO auth_user (
                            password, username, email, first_name, last_name, 
                            is_superuser, is_staff, is_active, date_joined
                        ) VALUES (%s, %s, %s, %s, %s, false, false, true, NOW())
                        RETURNING id
                    """, (hashed_password, username, email, f"First{i+1}", f"Last{i+1}"))
                    
                    user_id = cur.fetchone()[0]
                    users.append(user_id)
                    print(f"  ✅ Created user {username} (ID: {user_id})")
                else:
                    user_id = existing[0]
                    users.append(user_id)
                    print(f"  ⚠️ User {username} already exists (ID: {user_id})")
        
        conn.commit()
    
    print(f"\n✅ Total users: {len(users)}")
    return users

def generate_user_behaviors(users: List[int], listings_by_agency: Dict, num_behaviors: int = 10000):
    """Generate realistic browsing behaviors"""
    behaviors = []
    
    if not users:
        print("❌ No users found! Cannot generate behaviors.")
        return behaviors
    
    if not any(listings_by_agency.values()):
        print("❌ No listings found! Cannot generate behaviors.")
        return behaviors
    
    # Define user preferences (each user has favorite cities and agencies)
    user_prefs = {}
    cities = ['Tunis', 'La Marsa', 'Sousse', 'Sfax', 'Monastir', 'Ariana', 'Ben Arous']
    
    for user_id in users:
        user_prefs[user_id] = {
            'fav_cities': random.sample(cities, k=random.randint(1, 3)),
            'fav_agencies': random.sample(AGENCIES, k=random.randint(1, 3)),
            'price_range': (random.randint(200000, 500000), random.randint(500000, 2000000))
        }
    
    behavior_types = ['view', 'save', 'favorite', 'contact']
    behavior_weights = [0.7, 0.15, 0.1, 0.05]  # 70% views
    
    # Flatten all listings for random selection
    all_listings = []
    for agency_listings in listings_by_agency.values():
        all_listings.extend(agency_listings)
    
    if not all_listings:
        print("❌ No listings available!")
        return behaviors
    
    print(f"\n📊 Generating {num_behaviors} user behaviors...")
    
    for idx in range(num_behaviors):
        if idx % 2000 == 0:
            print(f"  Progress: {idx}/{num_behaviors} behaviors generated")
        
        user_id = random.choice(users)
        prefs = user_prefs[user_id]
        
        # 70% of the time, show listings matching preferences
        if random.random() < 0.7:
            # Pick a preferred agency
            agency = random.choice(prefs['fav_agencies'])
            agency_listings = listings_by_agency.get(agency, [])
            
            if agency_listings:
                # Filter by city and price
                matching = [
                    l for l in agency_listings
                    if l[1] in prefs['fav_cities']  # city match
                    and l[3] is not None  # price exists
                    and prefs['price_range'][0] <= l[3] <= prefs['price_range'][1]
                ]
                
                if matching:
                    listing = random.choice(matching)
                else:
                    listing = random.choice(agency_listings)
            else:
                listing = random.choice(all_listings)
        else:
            # Exploration - random listing
            listing = random.choice(all_listings)
        
        if not listing or len(listing) < 4:
            continue
        
        behavior = random.choices(behavior_types, weights=behavior_weights)[0]
        
        # Create event within last 90 days
        event_time = datetime.now() - timedelta(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        behaviors.append({
            'user_id': user_id,
            'listing_id': listing[0],
            'behavior_type': behavior,
            'duration_seconds': random.randint(10, 300) if behavior == 'view' else 0,
            'created_at': event_time
        })
    
    print(f"✅ Generated {len(behaviors)} behaviors")
    return behaviors

def insert_behaviors(behaviors: List[Dict]):
    """Insert behaviors into database"""
    if not behaviors:
        print("❌ No behaviors to insert!")
        return
    
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            for b in behaviors:
                try:
                    cur.execute("""
                        INSERT INTO user_listing_behaviors 
                        (user_id, listing_id, behavior_type, duration_seconds, created_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (b['user_id'], b['listing_id'], b['behavior_type'], 
                          b['duration_seconds'], b['created_at']))
                except Exception as e:
                    print(f"  ⚠️ Failed to insert behavior: {e}")
                    continue
        
        conn.commit()
    
    print(f"✅ Inserted {len(behaviors)} user behaviors into database")

def compute_listing_similarities(listings_by_agency: Dict):
    """Compute similarities between listings"""
    similarities = []
    
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            # First, clear old similarities
            cur.execute("TRUNCATE TABLE listing_recommendation_similarities")
            
            for agency in AGENCIES:
                listings = listings_by_agency.get(agency, [])
                print(f"  Computing similarities for {agency}: {len(listings)} listings")
                
                # Only compute for first 100 listings per agency to save time
                for i, listing1 in enumerate(listings[:100]):
                    for listing2 in listings[i+1:i+50]:
                        try:
                            # Calculate similarity based on features
                            score = 0
                            reasons = []
                            
                            # Same city
                            if listing1[1] and listing2[1] and listing1[1] == listing2[1]:
                                score += 0.4
                                reasons.append('same_city')
                            
                            # Same property type
                            if listing1[2] and listing2[2] and listing1[2] == listing2[2]:
                                score += 0.3
                                reasons.append('same_type')
                            
                            # Similar price (within 20%)
                            price1, price2 = listing1[3], listing2[3]
                            if price1 and price2 and price1 > 0 and price2 > 0:
                                price_ratio = min(price1, price2) / max(price1, price2)
                                if price_ratio > 0.8:
                                    score += 0.2 * price_ratio
                                    reasons.append('similar_price')
                            
                            # Similar surface
                            surface1, surface2 = listing1[4], listing2[4]
                            if surface1 and surface2 and surface1 > 0 and surface2 > 0:
                                surface_ratio = min(surface1, surface2) / max(surface1, surface2)
                                if surface_ratio > 0.7:
                                    score += 0.1 * surface_ratio
                                    reasons.append('similar_surface')
                            
                            if score > 0.3:  # Only store meaningful similarities
                                similarities.append((
                                    listing1[0], listing2[0], 
                                    min(score, 1.0), ','.join(reasons)
                                ))
                        except Exception as e:
                            continue
            
            # Insert similarities in batches
            if similarities:
                print(f"  Inserting {len(similarities)} similarities...")
                cur.executemany("""
                    INSERT INTO listing_recommendation_similarities 
                    (listing_id_1, listing_id_2, similarity_score, similarity_reason)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (listing_id_1, listing_id_2) DO UPDATE
                    SET similarity_score = EXCLUDED.similarity_score,
                        similarity_reason = EXCLUDED.similarity_reason,
                        computed_at = NOW()
                """, similarities)
        
        conn.commit()
    
    print(f"✅ Computed {len(similarities)} listing similarities")
    return similarities

def create_recommendation_tables():
    """Create the recommendation tables if they don't exist"""
    create_tables_sql = """
    -- User behaviors table
    CREATE TABLE IF NOT EXISTS user_listing_behaviors (
        id BIGSERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES auth_user(id) ON DELETE CASCADE,
        listing_id TEXT REFERENCES listings(id) ON DELETE CASCADE,
        behavior_type VARCHAR(30) NOT NULL,
        session_key VARCHAR(40),
        duration_seconds INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        CONSTRAINT valid_behavior CHECK (behavior_type IN ('view', 'save', 'favorite', 'contact', 'share'))
    );
    
    -- Listing similarities table
    CREATE TABLE IF NOT EXISTS listing_recommendation_similarities (
        listing_id_1 TEXT REFERENCES listings(id) ON DELETE CASCADE,
        listing_id_2 TEXT REFERENCES listings(id) ON DELETE CASCADE,
        similarity_score FLOAT CHECK (similarity_score >= 0 AND similarity_score <= 1),
        similarity_reason TEXT,
        computed_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (listing_id_1, listing_id_2)
    );
    
    -- User preferences cache
    CREATE TABLE IF NOT EXISTS user_recommendation_preferences (
        user_id INTEGER PRIMARY KEY REFERENCES auth_user(id) ON DELETE CASCADE,
        preferred_cities TEXT[],
        preferred_property_types TEXT[],
        min_price INTEGER,
        max_price INTEGER,
        min_surface INTEGER,
        max_surface INTEGER,
        preferred_agencies TEXT[],
        interaction_count INTEGER DEFAULT 0,
        last_active TIMESTAMPTZ,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_user_behaviors_user ON user_listing_behaviors(user_id);
    CREATE INDEX IF NOT EXISTS idx_user_behaviors_listing ON user_listing_behaviors(listing_id);
    CREATE INDEX IF NOT EXISTS idx_user_behaviors_created ON user_listing_behaviors(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_listing_sim_score ON listing_recommendation_similarities(similarity_score DESC);
    """
    
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            # Split and execute each statement
            for statement in create_tables_sql.split(';'):
                if statement.strip():
                    try:
                        cur.execute(statement)
                    except Exception as e:
                        print(f"  ⚠️ Table might already exist: {e}")
        conn.commit()
    
    print("✅ Recommendation tables ready")

# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Building Recommendation System")
    print("=" * 60)
    
    # Step 1: Create tables if they don't exist
    print("\n📋 Step 1: Creating recommendation tables...")
    create_recommendation_tables()
    
    # Step 2: Get your listings
    print("\n📊 Step 2: Fetching listings from database...")
    listings_by_agency = get_listings_by_agency()
    total_listings = sum(len(l) for l in listings_by_agency.values())
    print(f"\n✅ Found {total_listings} listings across {len(AGENCIES)} agencies")
    
    if total_listings == 0:
        print("❌ No listings found! Please check your database connection.")
        exit(1)
    
    # Step 3: Create mock users
    print("\n👥 Step 3: Creating mock users...")
    users = create_mock_users(50)
    
    if not users:
        print("❌ No users created! Cannot continue.")
        exit(1)
    
    # Step 4: Generate and insert behaviors
    print("\n🎯 Step 4: Generating user behaviors...")
    behaviors = generate_user_behaviors(users, listings_by_agency, 15000)
    
    if behaviors:
        insert_behaviors(behaviors)
    else:
        print("⚠️ No behaviors generated!")
    
    # Step 5: Compute listing similarities
    print("\n🔗 Step 5: Computing listing similarities...")
    compute_listing_similarities(listings_by_agency)
    
    # Step 6: Summary
    print("\n" + "=" * 60)
    print("✅ RECOMMENDATION SYSTEM READY!")
    print("=" * 60)
    print(f"📊 Statistics:")
    print(f"  - Users: {len(users)}")
    print(f"  - Listings: {total_listings}")
    print(f"  - Behaviors generated: {len(behaviors)}")
    print(f"  - Agencies: {len(AGENCIES)}")
    print("\n💡 Next steps:")
    print("  1. Run Django server to use recommendations")
    print("  2. Call /api/recommendations/ endpoint")
    print("  3. Track user behaviors automatically")
    print("=" * 60)