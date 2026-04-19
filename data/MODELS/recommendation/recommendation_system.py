#!/usr/bin/env python
"""
COMPLETE RECOMMENDATION SYSTEM - ONE FILE TO RULE THEM ALL
Run this once and you're done!
"""

import random
import psycopg2
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.decomposition import TruncatedSVD

# ============================================
# DATABASE CONNECTION
# ============================================

PG_CONN = dict(
    host="aws-1-eu-central-1.pooler.supabase.com",
    port=5432,
    dbname="postgres",
    user="postgres.amxnojlfczwffvtwutrb",
    password="capTEEMO5000",  # Your password
    sslmode="require"
)


# ============================================
# STEP 1: TRAIN THE MODEL
# ============================================

def train_model():
    """Train collaborative filtering model"""
    print("\n" + "="*60)
    print("🚀 TRAINING RECOMMENDATION MODEL")
    print("="*60)
    
    # Load data
    with psycopg2.connect(**PG_CONN) as conn:
        query = """
            SELECT 
                ub.user_id,
                ub.listing_id,
                CASE ub.behavior_type
                    WHEN 'view' THEN 1
                    WHEN 'save' THEN 3
                    WHEN 'favorite' THEN 5
                    WHEN 'contact' THEN 8
                    ELSE 1
                END as weight
            FROM user_listing_behaviors ub
            WHERE ub.created_at > NOW() - INTERVAL '90 days'
        """
        df = pd.read_sql(query, conn)
    
    print(f"📊 Loaded {len(df)} interactions")
    
    if len(df) < 50:
        print("❌ Not enough data! Need at least 50 interactions.")
        return None
    
    # Filter users with at least 3 interactions
    user_counts = df.groupby('user_id').size()
    active_users = user_counts[user_counts >= 3].index
    df = df[df['user_id'].isin(active_users)]
    
    # Filter listings with at least 2 interactions
    listing_counts = df.groupby('listing_id').size()
    active_listings = listing_counts[listing_counts >= 2].index
    df = df[df['listing_id'].isin(active_listings)]
    
    print(f"📊 After filtering: {len(df)} interactions")
    print(f"   Active users: {df['user_id'].nunique()}")
    print(f"   Active listings: {df['listing_id'].nunique()}")
    
    if df['user_id'].nunique() < 2 or df['listing_id'].nunique() < 2:
        print("❌ Need at least 2 users and 2 listings")
        return None
    
    # Build matrix
    user_ids = df['user_id'].unique()
    listing_ids = df['listing_id'].unique()
    user_to_idx = {uid: i for i, uid in enumerate(user_ids)}
    listing_to_idx = {lid: j for j, lid in enumerate(listing_ids)}
    
    rows = [user_to_idx[uid] for uid in df['user_id']]
    cols = [listing_to_idx[lid] for lid in df['listing_id']]
    data = df['weight'].values
    
    matrix = csr_matrix((data, (rows, cols)), 
                        shape=(len(user_ids), len(listing_ids)))
    
    print(f"🔨 Matrix shape: {matrix.shape}")
    sparsity = 100 * (1 - matrix.nnz / (matrix.shape[0] * matrix.shape[1]))
    print(f"   Sparsity: {sparsity:.2f}%")
    
    # Compute user similarity
    print("👥 Computing user similarity...")
    user_similarity = cosine_similarity(matrix)
    
    # Train SVD
    print("🎯 Training SVD...")
    n_components = min(20, min(matrix.shape) - 1)
    if n_components > 1:
        svd = TruncatedSVD(n_components=n_components, random_state=42)
        user_factors = svd.fit_transform(matrix)
        explained_var = svd.explained_variance_ratio_.sum()
        print(f"   Explained variance: {explained_var:.2%}")
    else:
        user_factors = None
        explained_var = 0
    
    # Evaluate
    print("\n📊 Evaluating model...")
    precision_scores = []
    recall_scores = []
    
    # Test on sample of users
    test_users = np.random.choice(user_ids, size=min(10, len(user_ids)), replace=False)
    
    for user_id in test_users:
        user_idx = user_to_idx[user_id]
        
        # Get user's items
        user_items = set(matrix[user_idx].indices)
        
        if len(user_items) == 0:
            continue
        
        # Split: 80% train, 20% test
        user_items_list = list(user_items)
        np.random.shuffle(user_items_list)
        split = int(0.8 * len(user_items_list))
        train_items = set(user_items_list[:split])
        test_items = set(user_items_list[split:])
        
        if len(test_items) == 0:
            continue
        
        # Find similar users based on training items
        similar_users = user_similarity[user_idx].argsort()[::-1][1:20]
        
        # Get recommendations
        recommendations = {}
        for sim_user in similar_users:
            sim_score = user_similarity[user_idx][sim_user]
            if sim_score < 0.1:
                continue
            sim_items = set(matrix[sim_user].indices)
            candidates = sim_items - train_items
            for item in candidates:
                recommendations[item] = recommendations.get(item, 0) + sim_score
        
        # Top 10 recommendations
        top_recs = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)[:10]
        recommended_items = set([item for item, _ in top_recs])
        
        # Calculate metrics
        hits = len(recommended_items & test_items)
        precision = hits / 10 if recommended_items else 0
        recall = hits / len(test_items) if test_items else 0
        
        precision_scores.append(precision)
        recall_scores.append(recall)
    
    avg_precision = np.mean(precision_scores) if precision_scores else 0
    avg_recall = np.mean(recall_scores) if recall_scores else 0
    f1 = 2 * (avg_precision * avg_recall) / (avg_precision + avg_recall + 1e-8)
    
    print("\n" + "="*60)
    print("✅ TRAINING COMPLETE")
    print("="*60)
    print(f"📈 Model Performance Metrics:")
    print(f"   Precision@10: {avg_precision:.3f} ({avg_precision*100:.1f}%)")
    print(f"   Recall@10:    {avg_recall:.3f} ({avg_recall*100:.1f}%)")
    print(f"   F1 Score:     {f1:.3f}")
    print(f"   Users tested: {len(precision_scores)}")
    print(f"   SVD Variance: {explained_var:.2%}")
    print("="*60)
    
    # Interpretation
    print("\n💡 Interpretation:")
    if avg_precision > 0.3:
        print("   ✅ Good! Model is making relevant recommendations")
    elif avg_precision > 0.15:
        print("   📈 Decent model, will improve with more data")
    else:
        print("   📊 Need more user behavior data for better accuracy")
    
    return {
        'precision': avg_precision,
        'recall': avg_recall,
        'f1': f1,
        'num_users': len(user_ids),
        'num_listings': len(listing_ids),
        'num_interactions': len(df)
    }

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("🏠 ESTATEMIND RECOMMENDATION SYSTEM")
    print("="*60)
    
    
    # Step 1: Train model
    results = train_model()
    
    if results:
        print("\n🎯 Next Steps:")
        print("   1. Use the model in your Django views")
        print("   2. Call get_recommendations(user_id) API")
        print("   3. Track more user behaviors to improve accuracy")
    else:
        print("\n❌ Training failed. Check:")
        print("   - Do you have users in auth_user?")
        print("   - Do you have listings?")
        print("   - Run generate_mock_behaviors() first")