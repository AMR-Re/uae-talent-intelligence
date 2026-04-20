"""
sync_db.py
─────────────────────────────────────────────────────────────────────────────
Copies ALL data from your local PostgreSQL into Supabase (cloud).
Run after loading locally to publish a fresh dataset to the cloud.

Usage:
    python sync_db.py
─────────────────────────────────────────────────────────────────────────────
"""

import time
from colorama import Fore, Style, init as colorama_init
import pandas as pd
from sqlalchemy import text

from etl.db import get_both_engines
from extraction.load_db import create_tables, load_jobs, load_skills, create_views, log_pipeline_run

colorama_init(autoreset=True)


def sync() -> None:
    print(f"\n{Fore.CYAN}── Sync: Local PostgreSQL → Supabase ────────────────────{Style.RESET_ALL}")
    start = time.time()

    local_engine, supabase_engine = get_both_engines()

    # ── Read from local ───────────────────────────────────────────────────────
    print(f"\n  {Fore.CYAN}Reading from local database...{Style.RESET_ALL}")
    with local_engine.connect() as conn:
        df_jobs   = pd.read_sql("SELECT * FROM jobs",       conn)
        df_skills = pd.read_sql("SELECT * FROM job_skills", conn)
        meta_row  = conn.execute(
            text("SELECT jobs_total, salary_coverage, avg_completeness "
                 "FROM pipeline_runs ORDER BY run_at DESC LIMIT 1")
        ).fetchone()

    print(f"    → {len(df_jobs):,}   jobs")
    print(f"    → {len(df_skills):,} skill tags")

    if df_jobs.empty:
        print(f"  {Fore.YELLOW}⚠  Local DB is empty. Run the pipeline first.{Style.RESET_ALL}")
        return

    # ── Write to Supabase ─────────────────────────────────────────────────────
    print(f"\n  {Fore.CYAN}Writing to Supabase...{Style.RESET_ALL}")

    create_tables(supabase_engine)

    # Drop auto-generated columns before insert
    df_jobs_insert   = df_jobs.drop(columns=["fetched_at"], errors="ignore")
    df_skills_insert = df_skills.drop(columns=["id"],       errors="ignore")

    n_jobs   = load_jobs(supabase_engine, df_jobs_insert)
    n_skills = load_skills(supabase_engine, df_skills_insert)

    print(f"    → Jobs synced  : {n_jobs:,}")
    print(f"    → Skills synced: {n_skills:,}")

    print(f"\n  {Fore.CYAN}Creating KPI views on Supabase...{Style.RESET_ALL}")
    create_views(supabase_engine)

    # Log the sync in Supabase pipeline_runs
    meta = {}
    if meta_row:
        meta = {
            "total_jobs":          meta_row[0],
            "salary_coverage_pct": meta_row[1],
            "avg_completeness":    meta_row[2],
        }
    log_pipeline_run(supabase_engine, meta, n_jobs, n_skills, status="sync")

    elapsed = round(time.time() - start, 1)
    print(f"\n  {Fore.GREEN}✅ Sync complete in {elapsed}s{Style.RESET_ALL}")
    print(f"     Supabase is now up to date.")
    print(f"     Power BI cloud refresh will pick up fresh data automatically.\n")


if __name__ == "__main__":
    sync()