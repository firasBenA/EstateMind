"""
EstateMind — Scorer Test Suite

Tests the new ML + XAI scorer in 4 stages:

    Stage 1 — Heuristic fallback (no dependencies needed)
    Stage 2 — ML training + SHAP (requires: pip install xgboost shap numpy)
    Stage 3 — Backward compatibility (pipeline.py / backfill won't break)
    Stage 4 — Edge cases (nulls, zeros, extreme values)

Run:
    python test_scorer.py              # all stages
    python test_scorer.py --stage 1    # heuristic only (no extra installs)
    python test_scorer.py --stage 2    # ML + SHAP
"""
from __future__ import annotations

import sys
import argparse
import traceback
from typing import Dict, Any

# ── Add your project root to path if needed ───────────────────────────────────
# Uncomment and adjust if scorer.py lives inside preprocessing/steps/
# sys.path.insert(0, "/path/to/your/project")

# If scorer.py is in the same folder as this test file:
import importlib.util, pathlib
_scorer_path = pathlib.Path(__file__).parent / "scorer.py"
spec = importlib.util.spec_from_file_location("scorer", _scorer_path)
scorer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scorer)


# ── Helpers ───────────────────────────────────────────────────────────────────

PASS = "✅ PASS"
FAIL = "❌ FAIL"
SKIP = "⏭  SKIP"


def check(label: str, condition: bool, detail: str = ""):
    status = PASS if condition else FAIL
    print(f"  {status}  {label}")
    if detail:
        print(f"         {detail}")
    return condition


def section(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


# ── Sample data ───────────────────────────────────────────────────────────────

SAMPLE_LISTINGS = [
    # HIGH quality
    {
        "price": 450000, "surface": 120, "rooms": 4,
        "city": "La Marsa", "region": "Tunis",
        "latitude": 36.87, "longitude": 10.32,
        "description": "Bel appartement S+3 avec vue mer et grande piscine chauffée",
        "image_count": 8, "features": ["piscine", "vue mer", "parking"],
        "municipalite": "La Marsa",
    },
    # GOOD quality
    {
        "price": 280000, "surface": 95, "rooms": 3,
        "city": "Sousse", "region": "Sousse",
        "latitude": 35.82, "longitude": 10.63,
        "description": "Appartement bien situé proche de la mer avec terrasse",
        "image_count": 4, "features": ["terrasse"],
        "municipalite": "Sousse Ville",
    },
    # LOW quality — missing several fields
    {
        "price": 150000, "surface": None, "rooms": None,
        "city": "Sfax", "region": "Sfax",
        "latitude": None, "longitude": None,
        "description": "Bien",
        "image_count": 0, "features": [],
    },
    # DROP — mostly nulls
    {
        "price": None, "surface": None, "rooms": None,
        "city": None, "region": "Tunis",
        "description": "Appartement",
        "image_count": 0, "features": [],
    },
    # Outlier — suspicious price
    {
        "price": 1000, "surface": 200, "rooms": 5,
        "city": "Tunis", "region": "Tunis",
        "latitude": 36.8, "longitude": 10.18,
        "description": "Villa luxueuse avec piscine jardin et vue panoramique",
        "image_count": 5, "features": ["piscine", "jardin"],
    },
    # Cross-verified + history bonus
    {
        "price": 320000, "surface": 110, "rooms": 4,
        "city": "Hammamet", "region": "Nabeul",
        "latitude": 36.4, "longitude": 10.61,
        "description": "Superbe villa plain pied avec jardin privatif et piscine",
        "image_count": 12, "features": ["piscine", "jardin"],
        "municipalite": "Hammamet",
    },
]

SAMPLE_FLAGS = [
    {"has_price_history": True, "cross_verified": True},                # HIGH
    {"has_price_history": True},                                         # GOOD
    {},                                                                   # LOW
    {},                                                                   # DROP
    {"price_outlier": True},                                             # outlier
    {"has_price_history": True, "cross_verified": True, "price_changed": True},  # bonus
]

EXPECTED_LEVELS = ["HIGH", "GOOD", "LOW", "DROP", None, None]


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 1 — Heuristic fallback (zero dependencies)
# ═════════════════════════════════════════════════════════════════════════════

def stage_1_heuristic():
    section("STAGE 1 — Heuristic Fallback (no ML deps needed)")
    passed = 0

    print("\n  [1.1] Basic scoring — all 6 sample listings\n")
    for i, (meta, flags, expected_level) in enumerate(
        zip(SAMPLE_LISTINGS, SAMPLE_FLAGS, EXPECTED_LEVELS)
    ):
        result = scorer.compute_score(meta, flags)

        print(f"  Listing {i+1}: {meta.get('city', '?')} / {meta.get('region', '?')}")
        print(f"    score={result['score']}/100  level={result['level']}  drop={result['should_drop']}")
        print(f"    {result['explanation']}")
        print()

        ok = check(
            f"Listing {i+1} has valid score range",
            0 <= result["score"] <= 100,
            f"got {result['score']}",
        )
        passed += ok

        ok = check(
            f"Listing {i+1} has level string",
            result["level"] in ("HIGH", "GOOD", "LOW", "DROP"),
            f"got {result['level']}",
        )
        passed += ok

        if expected_level:
            ok = check(
                f"Listing {i+1} expected level={expected_level}",
                result["level"] == expected_level,
                f"got {result['level']}",
            )
            passed += ok

    print("\n  [1.2] Return dict has all required keys")
    result = scorer.compute_score(SAMPLE_LISTINGS[0], SAMPLE_FLAGS[0])
    for key in ("score", "level", "should_drop", "breakdown", "explanation", "shap_values", "used_model"):
        passed += check(f"key '{key}' present", key in result)

    print("\n  [1.3] Heuristic mode (no ML model trained yet)")
    passed += check("used_model=False when no model trained", result["used_model"] == False)

    print("\n  [1.4] compute_model_weight()")
    for score, expected_w in [(90, 1.5), (70, 1.0), (40, 0.5), (10, 0.0)]:
        w = scorer.compute_model_weight(score)
        passed += check(f"score={score} → weight={expected_w}", w == expected_w, f"got {w}")

    print("\n  [1.5] batch_score()")
    results = scorer.batch_score(SAMPLE_LISTINGS)
    passed += check("batch_score returns correct count", len(results) == len(SAMPLE_LISTINGS))
    passed += check("batch_score adds reliability_score", "reliability_score" in results[0])
    passed += check("batch_score adds reliability_level", "reliability_level" in results[0])
    passed += check("batch_score adds score_explanation", "score_explanation" in results[0])

    return passed


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 2 — ML Training + SHAP
# ═════════════════════════════════════════════════════════════════════════════

def stage_2_ml():
    section("STAGE 2 — ML Training + SHAP Explainability")

    try:
        import numpy, xgboost, shap
    except ImportError as e:
        print(f"\n  {SKIP}  Missing dependency: {e}")
        print("  Install with: pip install xgboost shap numpy")
        return 0

    passed = 0

    # Generate a bigger synthetic dataset for training
    print("\n  [2.1] Generating synthetic training set (200 listings)...")
    import random
    random.seed(42)

    def make_listing(quality: str) -> Dict[str, Any]:
        """Generate a synthetic listing at a given quality level."""
        if quality == "high":
            return {
                "price": random.randint(300000, 600000),
                "surface": random.randint(100, 200),
                "rooms": random.randint(3, 5),
                "city": random.choice(["La Marsa", "Gammarth", "Carthage"]),
                "region": "Tunis",
                "latitude": 36.8 + random.uniform(-0.1, 0.1),
                "longitude": 10.3 + random.uniform(-0.1, 0.1),
                "description": "Superbe appartement avec toutes les commodités modernes et vue imprenable",
                "image_count": random.randint(5, 15),
                "features": ["piscine", "parking", "ascenseur"],
                "municipalite": "La Marsa",
                "_flags": {"has_price_history": True, "cross_verified": True},
            }
        elif quality == "medium":
            return {
                "price": random.randint(150000, 300000),
                "surface": random.randint(70, 120),
                "rooms": random.randint(2, 4),
                "city": random.choice(["Sousse", "Sfax", "Nabeul"]),
                "region": random.choice(["Sousse", "Sfax", "Nabeul"]),
                "latitude": 35.8 + random.uniform(-0.5, 0.5),
                "longitude": 10.5 + random.uniform(-0.5, 0.5),
                "description": "Appartement correct bien situé",
                "image_count": random.randint(2, 6),
                "features": ["parking"],
                "_flags": {},
            }
        else:  # low
            return {
                "price": random.choice([None, random.randint(50000, 100000)]),
                "surface": None,
                "rooms": None,
                "city": random.choice([None, "Tunis"]),
                "region": "Tunis",
                "description": "Bien",
                "image_count": 0,
                "features": [],
                "_flags": {},
            }

    train_records = (
        [make_listing("high")   for _ in range(70)] +
        [make_listing("medium") for _ in range(80)] +
        [make_listing("low")    for _ in range(50)]
    )
    passed += check("Generated 200 training records", len(train_records) == 200)

    print("\n  [2.2] Training ML model...")
    try:
        scorer.train_scorer(train_records, force=True)
        passed += check("train_scorer() ran without errors", True)
    except Exception as e:
        check("train_scorer() ran without errors", False, str(e))
        traceback.print_exc()
        return passed

    print("\n  [2.3] Model should now be active")
    result = scorer.compute_score(SAMPLE_LISTINGS[0], SAMPLE_FLAGS[0])
    passed += check("used_model=True after training", result["used_model"] == True)
    passed += check("Score still in 0-100 range",    0 <= result["score"] <= 100)
    passed += check("SHAP values are a dict",         isinstance(result["shap_values"], dict))
    passed += check("SHAP covers all features",       len(result["shap_values"]) == len(scorer.ALL_FEATURES))

    print("\n  [2.4] Feature importances")
    imp = scorer.get_feature_importances()
    passed += check("get_feature_importances() returns dict", isinstance(imp, dict))
    passed += check("importances cover all features", len(imp) == len(scorer.ALL_FEATURES))
    passed += check("importances sum to ~100%", abs(sum(imp.values()) - 100) < 1.0,
                    f"sum={sum(imp.values()):.1f}%")

    print("\n  Top 10 learned feature importances:")
    for feat, pct in sorted(imp.items(), key=lambda x: -x[1])[:10]:
        bar = "█" * int(pct / 2)
        print(f"    {feat:25s} {pct:5.1f}%  {bar}")

    print("\n  [2.5] SHAP explanation quality")
    result = scorer.compute_score(SAMPLE_LISTINGS[0], SAMPLE_FLAGS[0])
    passed += check("Explanation contains score",    str(result["score"]) in result["explanation"])
    passed += check("Explanation contains level",    result["level"] in result["explanation"])
    passed += check("Explanation mentions ML model", "ML model" in result["explanation"])
    print(f"\n  Sample explanation:\n  → {result['explanation']}")

    print("\n  [2.6] Scores are consistent (same input → same output)")
    r1 = scorer.compute_score(SAMPLE_LISTINGS[1], SAMPLE_FLAGS[1])
    r2 = scorer.compute_score(SAMPLE_LISTINGS[1], SAMPLE_FLAGS[1])
    passed += check("Deterministic output", r1["score"] == r2["score"])

    return passed


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 3 — Backward Compatibility
# ═════════════════════════════════════════════════════════════════════════════

def stage_3_compatibility():
    section("STAGE 3 — Backward Compatibility (pipeline.py / backfill)")
    passed = 0

    print("\n  Simulating how pipeline.py uses the scorer...\n")

    # Exactly how pipeline.py calls it (Step 6 in _step_score)
    for rec in SAMPLE_LISTINGS[:3]:
        flags = {
            "price_outlier":       rec.get("is_outlier", False),
            "suspected_duplicate": rec.get("suspected_duplicate", False),
            "nlp_enriched":        rec.get("nlp_enriched", False),
            "has_price_history":   rec.get("has_price_history", False),
            "price_changed":       rec.get("price_changed", False),
        }
        score_result = scorer.compute_score(rec, flags)

        # Exactly what pipeline.py reads from the result:
        rec["reliability_score"] = score_result["score"]
        rec["reliability_level"] = score_result["level"]
        rec["should_drop"]       = score_result["should_drop"]
        rec["model_weight"]      = scorer.compute_model_weight(score_result["score"])

        passed += check(
            f"pipeline.py usage — {rec.get('city','?')}",
            isinstance(rec["reliability_score"], int) and
            rec["reliability_level"] in ("HIGH", "GOOD", "LOW", "DROP") and
            isinstance(rec["should_drop"], bool) and
            rec["model_weight"] in (0.0, 0.5, 1.0, 1.5),
            f"score={rec['reliability_score']} level={rec['reliability_level']} weight={rec['model_weight']}"
        )

    print("\n  Simulating how backfill_reliability_scores.py uses the scorer...\n")

    # Exactly how backfill calls it
    for rec in SAMPLE_LISTINGS[:3]:
        flags = {
            "price_outlier":       rec.get("is_outlier", False),
            "suspected_duplicate": rec.get("suspected_duplicate", False),
            "nlp_enriched":        rec.get("nlp_enriched", False),
            "has_price_history":   rec.get("has_price_history", False),
            "price_changed":       rec.get("price_changed", False),
        }
        score_result = scorer.compute_score(rec, flags)
        rec["reliability_score"] = score_result["score"]
        rec["reliability_level"] = score_result["level"]
        rec["should_drop"]       = score_result["should_drop"]
        rec["model_weight"]      = scorer.compute_model_weight(score_result["score"])

        passed += check(
            f"backfill usage — {rec.get('city','?')}",
            all(k in rec for k in ("reliability_score", "reliability_level", "should_drop", "model_weight")),
        )

    return passed


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 4 — Edge Cases
# ═════════════════════════════════════════════════════════════════════════════

def stage_4_edge_cases():
    section("STAGE 4 — Edge Cases")
    passed = 0

    print()

    # Completely empty record
    result = scorer.compute_score({}, {})
    passed += check("Empty metadata → score=0, level=DROP",
                    result["score"] == 0 and result["level"] == "DROP",
                    f"got score={result['score']} level={result['level']}")

    # Price = 0
    result = scorer.compute_score({"price": 0}, {})
    passed += check("price=0 is penalized", result["score"] < 50,
                    f"got score={result['score']}")

    # All fields present + all bonuses
    perfect = {
        "price": 500000, "surface": 150, "rooms": 5,
        "city": "Tunis", "region": "Tunis",
        "latitude": 36.8, "longitude": 10.18,
        "description": "Magnifique propriété entièrement rénovée avec finitions haut de gamme",
        "image_count": 20, "features": ["piscine", "jardin", "parking", "ascenseur"],
        "municipalite": "Le Bardo",
    }
    result = scorer.compute_score(
        perfect,
        {"has_price_history": True, "cross_verified": True,
         "price_changed": True, "nlp_enriched": True}
    )
    passed += check("Perfect listing → HIGH or very high score",
                    result["score"] >= 60,
                    f"got score={result['score']} level={result['level']}")

    # Duplicate penalty
    result_normal = scorer.compute_score(SAMPLE_LISTINGS[0], {})
    result_dup    = scorer.compute_score(SAMPLE_LISTINGS[0], {"suspected_duplicate": True})
    passed += check("Duplicate flag lowers score",
                    result_dup["score"] < result_normal["score"],
                    f"normal={result_normal['score']} dup={result_dup['score']}")

    # Cross-verified bonus
    result_no_cv = scorer.compute_score(SAMPLE_LISTINGS[1], {})
    result_cv    = scorer.compute_score(SAMPLE_LISTINGS[1], {"cross_verified": True})
    passed += check("Cross-verified flag raises score",
                    result_cv["score"] >= result_no_cv["score"],
                    f"no_cv={result_no_cv['score']} cv={result_cv['score']}")

    # Score always clamped 0-100
    for i, (meta, flags) in enumerate(zip(SAMPLE_LISTINGS, SAMPLE_FLAGS)):
        result = scorer.compute_score(meta, flags)
        passed += check(f"Score clamped 0-100 (listing {i+1})",
                        0 <= result["score"] <= 100,
                        f"got {result['score']}")

    # should_drop is consistent with score
    for i, (meta, flags) in enumerate(zip(SAMPLE_LISTINGS, SAMPLE_FLAGS)):
        result = scorer.compute_score(meta, flags)
        expected_drop = result["score"] < 25
        passed += check(f"should_drop consistent with score (listing {i+1})",
                        result["should_drop"] == expected_drop)

    return passed


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Test EstateMind scorer")
    parser.add_argument("--stage", type=int, choices=[1, 2, 3, 4],
                        help="Run a single stage (default: all)")
    args = parser.parse_args()

    print("\n🏠  EstateMind — Scorer Test Suite")
    print("    scorer.py loaded from:", _scorer_path)

    total_passed = 0

    stages = {
        1: stage_1_heuristic,
        2: stage_2_ml,
        3: stage_3_compatibility,
        4: stage_4_edge_cases,
    }

    to_run = [args.stage] if args.stage else [1, 2, 3, 4]

    for s in to_run:
        try:
            total_passed += stages[s]()
        except Exception as e:
            print(f"\n  ❌ Stage {s} crashed: {e}")
            traceback.print_exc()

    print(f"\n{'=' * 60}")
    print(f"  Done. {total_passed} checks passed.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()