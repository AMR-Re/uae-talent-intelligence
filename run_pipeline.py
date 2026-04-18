"""
run_pipeline.py
Master runner — executes full ETL pipeline in one command.

Switch target database by changing DB_TARGET in .env:
  DB_TARGET=local      → loads into local PostgreSQL
  DB_TARGET=supabase   → loads directly into Supabase

Usage:
    python run_pipeline.py
"""

import os, sys, time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def banner(text: str):
    print(f"\n{'═'*55}")
    print(f"  {text}")
    print(f"{'═'*55}")


def step(label: str):
    print(f"\n{'─'*55}")
    print(f"  {label}")
    print(f"{'─'*55}")


def main():
    start  = time.time()
    target = os.getenv("DB_TARGET", "local")

    banner(f"UAE Talent Intelligence — ETL Pipeline\n"
           f"  Started  : {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n"
           f"  DB target: {target.upper()}")

    # step("1 / 3  — Data Ingestion (APIs)")
    # from etl.fetch_jobs import run_ingestion
    # run_ingestion()

    step("2 / 3  — Transform & Skill Extraction")
    from etl.transform import run_transform
    run_transform()

    step("3 / 3  — Load to PostgreSQL")
    from extraction.load_db import run_load
    run_load()

    elapsed = round(time.time() - start, 1)
    banner(f"✅ Pipeline complete in {elapsed}s\n"
           f"  Target DB: {target.upper()}\n"
           f"  Tip: run 'python sync_db.py' to mirror to the other DB")


if __name__ == "__main__":
    main()