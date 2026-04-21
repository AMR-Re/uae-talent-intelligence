"""
sync_db.py  (v2 — compatible with production load_db.py)
─────────────────────────────────────────────────────────────────────────────
Copies ALL data from your local PostgreSQL into Supabase (cloud).
Run after loading locally to publish a fresh dataset to the cloud.

Uses the same staging → DQ → UPSERT flow as the main pipeline, so the sync
is also idempotent and safe to run multiple times.

Usage:
    python sync_db.py
─────────────────────────────────────────────────────────────────────────────
"""

import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from colorama import Fore, Style, init as colorama_init
from sqlalchemy import text
from dotenv import load_dotenv

from etl.db import get_both_engines
from extraction.load_db import (
    SCHEMA_DDL,
    validate_dataframe,
    load_to_staging,
    run_dq_checks,
    merge_staging_to_production,
    load_skills,
    create_views,
    start_pipeline_run,
    finish_pipeline_run,
)

colorama_init(autoreset=True)
load_dotenv()


def sync() -> None:
    run_id     = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    wall_start = time.time()

    print(f"\n{Fore.CYAN}── Sync: Local PostgreSQL → Supabase ──────────────────────{Style.RESET_ALL}")
    print(f"  run_id : {run_id}")

    local_engine, supabase_engine = get_both_engines()

    # ── 1. Read from local ────────────────────────────────────────────────────
    print(f"\n  {Fore.CYAN}Reading from local database...{Style.RESET_ALL}")
    with local_engine.connect() as conn:
        df_jobs   = pd.read_sql("SELECT * FROM jobs",       conn)
        df_skills = pd.read_sql("SELECT * FROM job_skills", conn)
        meta_row  = conn.execute(
            text(
                "SELECT jobs_total, salary_coverage, avg_completeness "
                "FROM pipeline_runs ORDER BY COALESCE(start_time, run_at) DESC LIMIT 1"
            )
        ).fetchone()

    print(f"    → {len(df_jobs):,} jobs")
    print(f"    → {len(df_skills):,} skill tags")

    if df_jobs.empty:
        print(f"  {Fore.YELLOW}⚠  Local DB is empty. Run the pipeline first.{Style.RESET_ALL}")
        return

    # ── 2. Bootstrap Supabase schema ─────────────────────────────────────────
    print(f"\n  {Fore.CYAN}Creating / verifying Supabase schema...{Style.RESET_ALL}")
    with supabase_engine.begin() as conn:
        conn.execute(text(SCHEMA_DDL))
    print(f"  {Fore.GREEN}✓ Tables and indexes ready{Style.RESET_ALL}")

    # ── 3. Start pipeline_runs entry on Supabase ──────────────────────────────
    row_id = start_pipeline_run(supabase_engine, run_id, start_time)

    rows_inserted = rows_updated = n_skills = 0
    meta: dict = {}
    if meta_row:
        meta = {
            "total_jobs":          meta_row[0],
            "salary_coverage_pct": meta_row[1],
            "avg_completeness":    meta_row[2],
        }

    try:
        # ── 4. Validate ───────────────────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Validating data...{Style.RESET_ALL}")
        # Drop server-generated columns that must not be inserted
        df_jobs_clean = df_jobs.drop(columns=["fetched_at"], errors="ignore")
        df_jobs_clean, validation_issues = validate_dataframe(df_jobs_clean)

        # ── 5. Stage ──────────────────────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Loading to Supabase staging table...{Style.RESET_ALL}")
        load_to_staging(supabase_engine, df_jobs_clean, run_id)

        # ── 6. DQ checks ──────────────────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Running data quality checks...{Style.RESET_ALL}")
        run_dq_checks(supabase_engine, run_id, validation_issues)

        # ── 7. Merge staging → production (transactional UPSERT) ─────────────
        print(f"\n  {Fore.CYAN}Merging to Supabase jobs table (UPSERT)...{Style.RESET_ALL}")
        rows_inserted, rows_updated = merge_staging_to_production(supabase_engine, run_id)

        # ── 8. Skills ─────────────────────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Syncing skill tags...{Style.RESET_ALL}")
        df_skills_clean = df_skills.drop(columns=["id"], errors="ignore")
        n_skills = load_skills(supabase_engine, df_skills_clean)

        # ── 9. Views ──────────────────────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Creating KPI views on Supabase...{Style.RESET_ALL}")
        create_views(supabase_engine)

        # ── 10. Finish run record ─────────────────────────────────────────────
        finish_pipeline_run(
            supabase_engine, row_id, run_id, meta,
            rows_inserted, rows_updated, n_skills,
            status="sync",
        )

        elapsed = round(time.time() - wall_start, 1)
        print(f"\n  {Fore.GREEN}✅ Sync complete in {elapsed}s{Style.RESET_ALL}")
        print(f"     Inserted : {rows_inserted:,}")
        print(f"     Updated  : {rows_updated:,}")
        print(f"     Skills   : {n_skills:,}")
        print(f"     run_id   : {run_id}")
        print(f"     Power BI cloud refresh will pick up fresh data automatically.\n")

    except Exception as exc:
        finish_pipeline_run(
            supabase_engine, row_id, run_id, meta,
            rows_inserted, rows_updated, n_skills,
            status="sync_failed",
            error_message=str(exc)[:1000],
        )
        raise


if __name__ == "__main__":
    sync()