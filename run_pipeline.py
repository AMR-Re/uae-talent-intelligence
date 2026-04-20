"""
run_pipeline.py
─────────────────────────────────────────────────────────────────────────────
Master runner — executes the full ETL pipeline in sequence.

Stages:
  1. Fetch    — pull live job data from JSearch + Adzuna APIs
  2. Transform — clean, enrich, extract skills, derive features
  3. Salary ML — Tier 2 salary imputation (GBR model)
  4. Load     — upsert to PostgreSQL, create KPI views

Control via .env:
  DB_TARGET=local     → loads into local PostgreSQL
  DB_TARGET=supabase  → loads directly into Supabase cloud
  SKIP_FETCH=true     → reuse existing data/raw/ (skip API calls)

Usage:
  python run_pipeline.py
  python run_pipeline.py --skip-fetch      # reuse cached raw data
  python run_pipeline.py --skip-ml         # skip salary ML step
  python run_pipeline.py --transform-only  # steps 2+3 only
─────────────────────────────────────────────────────────────────────────────
"""

import sys
import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)
load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# CLI flags
# ─────────────────────────────────────────────────────────────────────────────
args        = set(sys.argv[1:])
SKIP_FETCH  = "--skip-fetch"  in args or os.getenv("SKIP_FETCH", "false").lower() == "true"
SKIP_ML     = "--skip-ml"     in args
TRANS_ONLY  = "--transform-only" in args


# ─────────────────────────────────────────────────────────────────────────────
# Pretty print helpers
# ─────────────────────────────────────────────────────────────────────────────
def banner(text: str) -> None:
    width = 60
    print(f"\n{Fore.CYAN}{'═' * width}{Style.RESET_ALL}")
    for line in text.split("\n"):
        print(f"  {line}")
    print(f"{Fore.CYAN}{'═' * width}{Style.RESET_ALL}")


def step_header(n: int, total: int, label: str) -> None:
    print(f"\n{Fore.BLUE}{'─' * 60}{Style.RESET_ALL}")
    print(f"  {Fore.BLUE}[{n}/{total}]{Style.RESET_ALL} {label}")
    print(f"{Fore.BLUE}{'─' * 60}{Style.RESET_ALL}")


def step_done(label: str, elapsed: float) -> None:
    print(f"\n  {Fore.GREEN}✓ {label} — {elapsed:.1f}s{Style.RESET_ALL}")


def step_skip(label: str) -> None:
    print(f"  {Fore.YELLOW}⊘ Skipping: {label}{Style.RESET_ALL}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    pipeline_start = time.time()
    target = os.getenv("DB_TARGET", "local").upper()
    now    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    banner(
        f"🇦🇪  UAE Talent Intelligence — ETL Pipeline\n"
        f"   Started  : {now}\n"
        f"   DB target: {target}\n"
        f"   Flags    : SKIP_FETCH={SKIP_FETCH}  SKIP_ML={SKIP_ML}  TRANSFORM_ONLY={TRANS_ONLY}"
    )

    total_steps = 4 if not TRANS_ONLY else 2

    # ── Stage 1: Fetch ────────────────────────────────────────────────────────
    step_header(1, total_steps, "Data Ingestion — JSearch + Adzuna APIs")
    t = time.time()
    if SKIP_FETCH:
        step_skip("Fetch stage (SKIP_FETCH=true) — using existing data/raw/ files")
    elif TRANS_ONLY:
        step_skip("Fetch stage (--transform-only)")
    else:
        from etl.fetch_jobs import run_ingestion
        run_ingestion()
        step_done("Ingestion", time.time() - t)

    # ── Stage 2: Transform ────────────────────────────────────────────────────
    step_header(2, total_steps, "Transform — Clean · Enrich · Extract · Impute (Tier 1)")
    t = time.time()
    from etl.transform import run_transform
    run_transform()
    step_done("Transform", time.time() - t)

    # ── Stage 3: Salary ML ────────────────────────────────────────────────────
    step_header(3, total_steps, "Salary Model — Gradient Boosting Imputation (Tier 2)")
    t = time.time()
    if SKIP_ML:
        step_skip("Salary ML (--skip-ml)")
    else:
        from etl.salary_model import train_and_impute
        train_and_impute()
        step_done("Salary Model", time.time() - t)

    # ── Stage 4: Load ─────────────────────────────────────────────────────────
    if not TRANS_ONLY:
        step_header(4, total_steps, f"Load — Upsert to PostgreSQL ({target})")
        t = time.time()
        from extraction.load_db import run_load
        run_load()
        step_done("Load", time.time() - t)

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = round(time.time() - pipeline_start, 1)
    banner(
        f"✅  Pipeline complete in {elapsed}s\n"
        f"   Target DB : {target}\n"
        f"   Next step : Open Power BI → Refresh data\n"
        f"   Sync tip  : Run python sync_db.py to mirror local → Supabase"
    )


if __name__ == "__main__":
    main()