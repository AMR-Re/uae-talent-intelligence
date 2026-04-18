"""
sync_db.py
Copies all data from your local PostgreSQL into Supabase.
Run this after you've loaded locally and want to push to the cloud.

Usage:
    python sync_db.py
"""

import pandas as pd
from sqlalchemy import text
from etl.db import get_both_engines
from extraction.load_db import create_tables, load_jobs, load_skills, create_views


def sync():
    print("\n── Sync: Local PostgreSQL → Supabase ───────────────")

    local_engine, supabase_engine = get_both_engines()

    # ── Read everything from local ──────────────────────────
    print("\n  Reading from local database...")
    with local_engine.connect() as conn:
        df_jobs   = pd.read_sql("SELECT * FROM jobs",       conn)
        df_skills = pd.read_sql("SELECT * FROM job_skills", conn)

    print(f"    → {len(df_jobs)} jobs")
    print(f"    → {len(df_skills)} skill tags")

    if df_jobs.empty:
        print("  ⚠  Local database is empty. Run the pipeline first.")
        return

    # ── Write to Supabase ───────────────────────────────────
    print("\n  Writing to Supabase...")

    print("  Creating tables...")
    create_tables(supabase_engine)

    # Drop the auto-generated columns that aren't needed for insert
    df_jobs_insert = df_jobs.drop(columns=["fetched_at"], errors="ignore")
    df_skills_insert = df_skills.drop(columns=["id"], errors="ignore")

    n_jobs   = load_jobs(supabase_engine, df_jobs_insert)
    n_skills = load_skills(supabase_engine, df_skills_insert)

    print(f"    → Jobs synced   : {n_jobs}")
    print(f"    → Skills synced : {n_skills}")

    print("  Creating KPI views...")
    create_views(supabase_engine)

    print("\n✅ Sync complete — Supabase is up to date.")
    print("   Power BI (cloud) and GitHub Actions will now use fresh data.\n")


if __name__ == "__main__":
    sync()