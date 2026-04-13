"""
EstateMind — Preprocessing Pipeline (pgvector version)

Remplace Pinecone par pgvector.
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Dict, Any, List

from loguru import logger

from preprocessing.steps.normalizer        import batch_normalize
from preprocessing.steps.null_handler      import batch_handle_nulls, null_report
from preprocessing.steps.outlier_detector import batch_flag_outliers
from preprocessing.steps.scorer            import batch_score
from preprocessing.steps.deduplicator      import find_duplicates_in_batch, dedup_report
from preprocessing.steps.change_detector   import batch_process_changes, get_change_summary


EXPORT_DIR        = os.environ.get("EXPORT_DIR", "data/exports")
MIN_SCORE_TO_KEEP = 25


class PreprocessingPipeline:

    def __init__(self, vector_db=None, force_reprocess: bool = False):
        self.vector_db = vector_db
        self.force_reprocess = force_reprocess

    def run(self, export: bool = True) -> Dict[str, Any]:
        start_time = time.time()
        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        logger.info(f"[Pipeline] Starting preprocessing run {run_id}")

        report = {"run_id": run_id, "started_at": datetime.utcnow().isoformat(), "steps": {}}

        records = self._step_fetch(report)
        if not records:
            logger.warning("[Pipeline] No records fetched — aborting")
            return report

        records = self._step_normalize(records, report)
        records = self._step_fill_nulls(records, report)
        records = self._step_deduplicate(records, report)
        records = self._step_flag_outliers(records, report)
        records = self._step_score(records, report)
        records = self._step_detect_changes(records, report)
        self._step_upsert(records, report)

        if export:
            self._step_export(records, report, run_id)

        elapsed = round(time.time() - start_time, 1)
        report["elapsed_s"]     = elapsed
        report["finished_at"]   = datetime.utcnow().isoformat()
        report["total_records"] = len(records)

        logger.info(f"[Pipeline] Run {run_id} complete in {elapsed}s — {len(records)} records")
        self._log_summary(report)
        return report

    def _step_fetch(self, report):
        logger.info("[Pipeline] Step 1: Fetching records from PostgreSQL")
        try:
            if self.vector_db is None:
                return []
            records = self.vector_db.fetch_all_metadata(limit=10000)
            report["steps"]["fetch"] = {"status": "ok", "fetched": len(records)}
            return records
        except Exception as e:
            logger.error(f"[Pipeline] Fetch failed: {e}")
            report["steps"]["fetch"] = {"status": "error", "error": str(e)}
            return []

    def _step_normalize(self, records, report):
        logger.info("[Pipeline] Step 2: Normalizing")
        try:
            result = batch_normalize(records)
            changed = sum(1 for r in result if r.get("normalized"))
            report["steps"]["normalize"] = {"status": "ok", "changed": changed}
            return result
        except Exception as e:
            logger.error(f"[Pipeline] Normalize failed: {e}")
            report["steps"]["normalize"] = {"status": "error", "error": str(e)}
            return records

    def _step_fill_nulls(self, records, report):
        logger.info("[Pipeline] Step 3: Filling nulls")
        try:
            null_rpt = null_report(records)
            result = batch_handle_nulls(records)
            enriched = sum(1 for r in result if r.get("nlp_enriched"))
            report["steps"]["null_handler"] = {"status": "ok", "enriched": enriched}
            return result
        except Exception as e:
            logger.error(f"[Pipeline] Null handler failed: {e}")
            report["steps"]["null_handler"] = {"status": "error", "error": str(e)}
            return records

def _step_deduplicate(self, records, report):
    logger.info("[Pipeline] Step 4: Deduplication")
    try:
        dupes = find_duplicates_in_batch(records)
        dupe_ids = set()
        for d in dupes:
            did = d.get("duplicate_id") or d.get("id") or d.get("property_id")
            if did:
                dupe_ids.add(did)
        for r in records:
            if r.get("property_id") in dupe_ids:
                r["suspected_duplicate"] = True
        report["steps"]["deduplication"] = {"status": "ok", "duplicates_found": len(dupes)}
        return records
    except Exception as e:
        logger.error(f"[Pipeline] Deduplication failed: {e}")
        report["steps"]["deduplication"] = {"status": "error", "error": str(e)}
        return records

    def _step_flag_outliers(self, records, report):
        logger.info("[Pipeline] Step 5: Flagging outliers")
        try:
            result = batch_flag_outliers(records)
            flagged = sum(1 for r in result if r.get("is_outlier"))
            report["steps"]["outlier_detector"] = {"status": "ok", "flagged": flagged}
            return result
        except Exception as e:
            logger.error(f"[Pipeline] Outlier detection failed: {e}")
            report["steps"]["outlier_detector"] = {"status": "error", "error": str(e)}
            return records

    def _step_score(self, records, report):
        logger.info("[Pipeline] Step 6: Scoring")
        try:
            from preprocessing.steps.scorer import compute_model_weight
            scored = []
            for rec in records:
                res = batch_score([rec])
                score_result = res[0] if res else {}
                rec["reliability_score"] = score_result.get("reliability_score", 0)
                rec["reliability_level"] = score_result.get("reliability_level", "UNKNOWN")
                rec["should_drop"]       = score_result.get("should_drop", False)
                rec["model_weight"]      = compute_model_weight(score_result.get("score", 0))
                scored.append(rec)
            levels = {}
            for r in scored:
                lvl = r.get("reliability_level", "UNKNOWN")
                levels[lvl] = levels.get(lvl, 0) + 1
            report["steps"]["scorer"] = {"status": "ok", "score_distribution": levels}
            return scored
        except Exception as e:
            logger.error(f"[Pipeline] Scoring failed: {e}")
            report["steps"]["scorer"] = {"status": "error", "error": str(e)}
            return records

    def _step_detect_changes(self, records, report):
        logger.info("[Pipeline] Step 7: Change detection")
        try:
            result = batch_process_changes(records)
            summary = get_change_summary()
            report["steps"]["change_detector"] = {"status": "ok", "summary": summary}
            return result
        except Exception as e:
            logger.error(f"[Pipeline] Change detection failed: {e}")
            report["steps"]["change_detector"] = {"status": "error", "error": str(e)}
            return records

    def _step_upsert(self, records, report):
        logger.info("[Pipeline] Step 8: Upserting to PostgreSQL")
        if self.vector_db is None:
            report["steps"]["upsert"] = {"status": "skipped"}
            return
        try:
            upserted = errors = 0
            conn = self.vector_db.conn
            for rec in records:
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE listings SET
                                etl_status = 'processed',
                                etl_processed_at = NOW()
                            WHERE source_name = %s AND property_id = %s
                        """, (rec.get("source_name"), rec.get("property_id")))
                    conn.commit()
                    upserted += 1
                except Exception as e:
                    conn.rollback()
                    errors += 1
            report["steps"]["upsert"] = {"status": "ok", "upserted": upserted, "errors": errors}
        except Exception as e:
            logger.error(f"[Pipeline] Upsert failed: {e}")
            report["steps"]["upsert"] = {"status": "error", "error": str(e)}

    def _step_export(self, records, report, run_id):
        logger.info("[Pipeline] Step 9: Exporting")
        try:
            import csv
            os.makedirs(EXPORT_DIR, exist_ok=True)
            all_path   = os.path.join(EXPORT_DIR, f"clean_{run_id}.csv")
            model_path = os.path.join(EXPORT_DIR, f"model_ready_{run_id}.csv")
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
                "is_outlier", "suspected_duplicate", "scraped_at",
            ]
            for path, data in [(all_path, records), (model_path, model_records)]:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                    writer.writeheader()
                    for r in data:
                        row = dict(r)
                        if isinstance(row.get("features"), list):
                            row["features"] = "|".join(str(x) for x in row["features"])
                        writer.writerow(row)
            report["steps"]["export"] = {
                "status": "ok",
                "all_records": len(records),
                "model_records": len(model_records),
            }
        except Exception as e:
            logger.error(f"[Pipeline] Export failed: {e}")
            report["steps"]["export"] = {"status": "error", "error": str(e)}

    def _log_summary(self, report):
        logger.info("=" * 60)
        for step, result in report.get("steps", {}).items():
            logger.info(f"  {step:20s} → {result.get('status', '?')}")
        logger.info(f"  Total time: {report.get('elapsed_s')}s")
        logger.info("=" * 60)