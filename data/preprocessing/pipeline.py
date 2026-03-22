"""
EstateMind — Preprocessing Pipeline

Orchestrates all data cleaning steps in the correct order.
Runs after each scraping cycle (every 24 hours).

Pipeline order:
1. Fetch all records from Pinecone
2. Normalize field formats
3. Fill nulls via NLP extraction
4. Detect cross-source duplicates
5. Flag outliers (statistical + absolute bounds)
6. Compute reliability scores
7. Detect price changes + write to SQLite time series
8. Upsert cleaned metadata back to Pinecone
9. Export clean snapshot for modeling

Each step is independent — if one fails, the pipeline continues.
"""
from __future__ import annotations

import os
import json
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

from loguru import logger

from preprocessing.steps.normalizer        import batch_normalize
from preprocessing.steps.null_handler      import batch_handle_nulls, null_report
from preprocessing.steps.outlier_detector import batch_flag_outliers
from preprocessing.steps.scorer            import batch_score
from preprocessing.steps.deduplicator      import find_duplicates_in_batch, dedup_report
from preprocessing.steps.change_detector   import batch_process_changes, get_change_summary


# ── Config ────────────────────────────────────────────────────────────────────

PINECONE_FETCH_LIMIT   = 10_000   # max vectors to fetch per run
UPSERT_BATCH_SIZE      = 100      # Pinecone upsert batch size
EXPORT_DIR             = os.environ.get("EXPORT_DIR", "data/exports")
MIN_SCORE_TO_KEEP      = 25       # drop listings below this score from exports


class PreprocessingPipeline:
    """
    Full data cleaning pipeline for EstateMind.

    Usage:
        pipeline = PreprocessingPipeline(vector_db)
        report = pipeline.run()
    """

    def __init__(self, vector_db=None):
        """
        Args:
            vector_db: VectorDBHandler instance (optional — can run without Pinecone
                       for testing, but won't fetch/upsert)
        """
        self.vector_db = vector_db
        self.run_stats: Dict[str, Any] = {}

    def run(self, export: bool = True) -> Dict[str, Any]:
        """
        Run the full pipeline.

        Args:
            export: whether to export clean snapshot CSV after cleaning

        Returns:
            Pipeline run report with stats from each step
        """
        start_time = time.time()
        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        logger.info(f"[Pipeline] Starting preprocessing run {run_id}")

        report = {
            "run_id":     run_id,
            "started_at": datetime.utcnow().isoformat(),
            "steps":      {},
        }

        # ── Step 1: Fetch from Pinecone ───────────────────────────────────────
        records = self._step_fetch(report)
        if not records:
            logger.warning("[Pipeline] No records fetched — aborting")
            return report

        # ── Step 2: Normalize ─────────────────────────────────────────────────
        records = self._step_normalize(records, report)

        # ── Step 3: Fill nulls via NLP ────────────────────────────────────────
        records = self._step_fill_nulls(records, report)

        # ── Step 4: Detect duplicates ─────────────────────────────────────────
        records = self._step_deduplicate(records, report)

        # ── Step 5: Flag outliers ─────────────────────────────────────────────
        records = self._step_flag_outliers(records, report)

        # ── Step 6: Compute reliability scores ────────────────────────────────
        records = self._step_score(records, report)

        # ── Step 7: Change detection → SQLite ────────────────────────────────
        records = self._step_detect_changes(records, report)

        # ── Step 8: Upsert back to Pinecone ──────────────────────────────────
        self._step_upsert(records, report)

        # ── Step 9: Export clean snapshot ────────────────────────────────────
        if export:
            self._step_export(records, report, run_id)

        # ── Final report ──────────────────────────────────────────────────────
        elapsed = round(time.time() - start_time, 1)
        report["elapsed_s"]    = elapsed
        report["finished_at"]  = datetime.utcnow().isoformat()
        report["total_records"] = len(records)

        logger.info(
            f"[Pipeline] Run {run_id} complete in {elapsed}s — "
            f"{len(records)} records processed"
        )
        self._log_summary(report)
        return report

    # ── Step implementations ──────────────────────────────────────────────────

    def _step_fetch(self, report: Dict) -> List[Dict]:
        logger.info("[Pipeline] Step 1: Fetching records from Pinecone")
        try:
            if self.vector_db is None:
                logger.warning("[Pipeline] No vector_db — using empty list")
                return []
            records, total = self.vector_db._fetch_all_metadata(
                limit=PINECONE_FETCH_LIMIT
            )
            report["steps"]["fetch"] = {
                "status":  "ok",
                "fetched": len(records),
                "total":   total,
            }
            logger.info(f"[Pipeline] Fetched {len(records)} records")
            return records
        except Exception as e:
            logger.error(f"[Pipeline] Fetch failed: {e}")
            report["steps"]["fetch"] = {"status": "error", "error": str(e)}
            return []

    def _step_normalize(self, records: List, report: Dict) -> List:
        logger.info("[Pipeline] Step 2: Normalizing field formats")
        try:
            result = batch_normalize(records)
            changed = sum(1 for r in result if r.get("normalized"))
            report["steps"]["normalize"] = {
                "status":  "ok",
                "changed": changed,
            }
            logger.info(f"[Pipeline] Normalized {changed}/{len(result)} records")
            return result
        except Exception as e:
            logger.error(f"[Pipeline] Normalize failed: {e}")
            report["steps"]["normalize"] = {"status": "error", "error": str(e)}
            return records

    def _step_fill_nulls(self, records: List, report: Dict) -> List:
        logger.info("[Pipeline] Step 3: Filling nulls via NLP")
        try:
            # Log null report before filling
            null_rpt = null_report(records)
            logger.info(
                f"[Pipeline] Null analysis — top issues: "
                f"{[(f, s['null_pct']) for f, s in null_rpt.get('most_problematic', [])]}"
            )
            result = batch_handle_nulls(records)
            enriched = sum(1 for r in result if r.get("nlp_enriched"))
            report["steps"]["null_handler"] = {
                "status":   "ok",
                "enriched": enriched,
                "null_analysis": null_rpt.get("field_null_analysis", {}),
            }
            return result
        except Exception as e:
            logger.error(f"[Pipeline] Null handler failed: {e}")
            report["steps"]["null_handler"] = {"status": "error", "error": str(e)}
            return records

    def _step_deduplicate(self, records: List, report: Dict) -> List:
        logger.info("[Pipeline] Step 4: Cross-source deduplication")
        try:
            result = find_duplicates_in_batch(records)
            dup_rpt = dedup_report(result)
            report["steps"]["deduplication"] = {
                "status": "ok",
                **dup_rpt,
            }
            logger.info(
                f"[Pipeline] Dedup: {dup_rpt['suspected_duplicates']} duplicates "
                f"({dup_rpt['duplicate_rate_pct']}%)"
            )
            return result
        except Exception as e:
            logger.error(f"[Pipeline] Dedup failed: {e}")
            report["steps"]["deduplication"] = {"status": "error", "error": str(e)}
            return records

    def _step_flag_outliers(self, records: List, report: Dict) -> List:
        logger.info("[Pipeline] Step 5: Flagging outliers")
        try:
            result = batch_flag_outliers(records, build_stats_from_batch=True)
            flagged = sum(1 for r in result if r.get("is_outlier"))
            report["steps"]["outlier_detector"] = {
                "status":  "ok",
                "flagged": flagged,
                "flagged_pct": round(flagged / len(result) * 100, 1) if result else 0,
            }
            return result
        except Exception as e:
            logger.error(f"[Pipeline] Outlier detection failed: {e}")
            report["steps"]["outlier_detector"] = {"status": "error", "error": str(e)}
            return records

    def _step_score(self, records: List, report: Dict) -> List:
        logger.info("[Pipeline] Step 6: Computing reliability scores")
        try:
            # Build flags from previous steps for scorer
            scored_records = []
            for rec in records:
                flags = {
                    "price_outlier":       rec.get("is_outlier", False),
                    "suspected_duplicate": rec.get("suspected_duplicate", False),
                    "nlp_enriched":        rec.get("nlp_enriched", False),
                    "has_price_history":   rec.get("has_price_history", False),
                    "price_changed":       rec.get("price_changed", False),
                }
                from preprocessing.steps.scorer import compute_score, compute_model_weight
                score_result = compute_score(rec, flags)
                rec["reliability_score"] = score_result["score"]
                rec["reliability_level"] = score_result["level"]
                rec["should_drop"]       = score_result["should_drop"]
                rec["model_weight"]      = compute_model_weight(score_result["score"])
                scored_records.append(rec)

            levels = {}
            for r in scored_records:
                lvl = r.get("reliability_level", "UNKNOWN")
                levels[lvl] = levels.get(lvl, 0) + 1

            report["steps"]["scorer"] = {
                "status": "ok",
                "score_distribution": levels,
                "drop_count": levels.get("DROP", 0),
            }
            logger.info(f"[Pipeline] Score distribution: {levels}")
            return scored_records
        except Exception as e:
            logger.error(f"[Pipeline] Scoring failed: {e}")
            report["steps"]["scorer"] = {"status": "error", "error": str(e)}
            return records

    def _step_detect_changes(self, records: List, report: Dict) -> List:
        logger.info("[Pipeline] Step 7: Detecting price changes → SQLite")
        try:
            result = batch_process_changes(records)
            summary = get_change_summary()
            report["steps"]["change_detector"] = {
                "status":  "ok",
                "summary": summary,
            }
            return result
        except Exception as e:
            logger.error(f"[Pipeline] Change detection failed: {e}")
            report["steps"]["change_detector"] = {"status": "error", "error": str(e)}
            return records

    def _step_upsert(self, records: List, report: Dict) -> None:
        logger.info("[Pipeline] Step 8: Upserting cleaned metadata to Pinecone")
        if self.vector_db is None:
            report["steps"]["upsert"] = {"status": "skipped", "reason": "no vector_db"}
            return
        try:
            upserted = 0
            errors   = 0
            # Fields to upsert back — only metadata, not vectors (vectors unchanged)
            clean_fields = [
                "price", "surface", "rooms", "city", "region", "zone",
                "municipalite", "transaction_type", "type", "features",
                "normalized", "nlp_enriched", "nlp_filled_fields",
                "reliability_score", "reliability_level", "should_drop",
                "model_weight", "is_outlier", "outlier_flags",
                "suspected_duplicate", "canonical_id",
                "change_type", "price_delta", "price_delta_pct",
                "has_price_history", "price_per_m2",
            ]
            # Batch upsert metadata updates
            batch = []
            for rec in records:
                vector_id = f"{rec.get('source_name')}:{rec.get('property_id')}"
                clean_meta = {
                    k: v for k, v in rec.items()
                    if k in clean_fields and v is not None
                }
                batch.append((vector_id, clean_meta))

                if len(batch) >= UPSERT_BATCH_SIZE:
                    ok, err = self._upsert_batch(batch)
                    upserted += ok; errors += err; batch = []

            if batch:
                ok, err = self._upsert_batch(batch)
                upserted += ok; errors += err

            report["steps"]["upsert"] = {
                "status":   "ok",
                "upserted": upserted,
                "errors":   errors,
            }
            logger.info(f"[Pipeline] Upserted {upserted} records, {errors} errors")
        except Exception as e:
            logger.error(f"[Pipeline] Upsert failed: {e}")
            report["steps"]["upsert"] = {"status": "error", "error": str(e)}

    def _upsert_batch(self, batch: List) -> tuple:
        """Upsert a batch of (vector_id, metadata) pairs to Pinecone."""
        ok = err = 0
        try:
            index = self.vector_db.index
            vectors_to_upsert = []
            for vector_id, metadata in batch:
                # Fetch existing vector to avoid overwriting it
                try:
                    existing = index.fetch(ids=[vector_id])
                    if existing and existing.vectors:
                        vec = existing.vectors[vector_id].values
                        # Clean metadata — Pinecone rejects None values
                        clean = {
                            k: v for k, v in metadata.items()
                            if v is not None and not isinstance(v, (dict,))
                            and (not isinstance(v, list) or all(
                                isinstance(x, (str, int, float, bool)) for x in v
                            ))
                        }
                        vectors_to_upsert.append({
                            "id": vector_id,
                            "values": vec,
                            "metadata": clean,
                        })
                        ok += 1
                except Exception:
                    err += 1

            if vectors_to_upsert:
                index.upsert(vectors=vectors_to_upsert)
        except Exception as e:
            logger.error(f"[Pipeline] Batch upsert error: {e}")
            err += len(batch)
        return ok, err

    def _step_export(
        self, records: List, report: Dict, run_id: str
    ) -> None:
        logger.info("[Pipeline] Step 9: Exporting clean snapshot")
        try:
            import csv
            os.makedirs(EXPORT_DIR, exist_ok=True)

            # Export all records (including low-score ones, flagged)
            all_path = os.path.join(EXPORT_DIR, f"clean_{run_id}.csv")
            # Export only modeling-ready records (score >= MIN_SCORE_TO_KEEP)
            model_path = os.path.join(EXPORT_DIR, f"model_ready_{run_id}.csv")

            all_records = records
            model_records = [
                r for r in records
                if (r.get("reliability_score") or 0) >= MIN_SCORE_TO_KEEP
                and not r.get("suspected_duplicate")
            ]

            fields = [
                "property_id", "source_name", "price", "surface", "rooms",
                "city", "region", "zone", "municipalite", "transaction_type",
                "type", "latitude", "longitude", "features",
                "reliability_score", "reliability_level", "model_weight",
                "is_outlier", "suspected_duplicate", "change_type",
                "price_delta_pct", "price_per_m2", "scraped_at",
            ]

            for path, data in [(all_path, all_records), (model_path, model_records)]:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                    writer.writeheader()
                    for r in data:
                        # Serialize lists to strings for CSV
                        row = dict(r)
                        if isinstance(row.get("features"), list):
                            row["features"] = "|".join(row["features"])
                        writer.writerow(row)

            report["steps"]["export"] = {
                "status":        "ok",
                "all_records":   len(all_records),
                "model_records": len(model_records),
                "all_path":      all_path,
                "model_path":    model_path,
            }
            logger.info(
                f"[Pipeline] Exported {len(all_records)} total, "
                f"{len(model_records)} model-ready records"
            )
        except Exception as e:
            logger.error(f"[Pipeline] Export failed: {e}")
            report["steps"]["export"] = {"status": "error", "error": str(e)}

    def _log_summary(self, report: Dict) -> None:
        logger.info("=" * 60)
        logger.info(f"[Pipeline] Run {report['run_id']} Summary:")
        for step, result in report.get("steps", {}).items():
            status = result.get("status", "?")
            logger.info(f"  {step:20s} → {status}")
        logger.info(f"  Total time: {report.get('elapsed_s')}s")
        logger.info("=" * 60)


# ── __init__.py content for preprocessing ────────────────────────────────────

INIT_CONTENT = '"""EstateMind preprocessing pipeline."""\n'