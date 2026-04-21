"""
run_pipeline.py  (v2 — production-grade)
─────────────────────────────────────────────────────────────────────────────
Master runner — executes the full ETL pipeline in sequence.

Stages:
  1. Fetch      — pull live job data from JSearch + Adzuna APIs
  2. Transform  — clean, enrich, extract skills, derive features
  3. Salary ML  — Tier 2 salary imputation (GBR model)
  4. Load       — staging → DQ checks → UPSERT to PostgreSQL; create views

New in v2:
  - Generates a UUID run_id at startup, threaded through every stage so
    every record is traceable to the exact pipeline run that created it.
  - Records pipeline start in pipeline_runs (status='running') before any
    stage executes; updates to 'success' or 'failed' at end.
  - All stage exceptions are caught, logged, and the pipeline_runs status
    is set to 'failed' with a structured error_message.
  - Structured log file written to logs/pipeline_<date>.log.

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

import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from colorama import Fore, Style, init as colorama_init
from dotenv import load_dotenv

colorama_init(autoreset=True)
load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# Logging setup  (structured file log + coloured console)
# ─────────────────────────────────────────────────────────────────────────────

def _setup_logging(run_id: str) -> None:
    Path("logs").mkdir(exist_ok=True)
    date_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file  = f"logs/pipeline_{date_str}.log"

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    logging.info(f"Pipeline started  run_id={run_id}")
    print(f"  {Fore.CYAN}Logs → {log_file}{Style.RESET_ALL}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI flags
# ─────────────────────────────────────────────────────────────────────────────

args        = set(sys.argv[1:])
SKIP_FETCH  = "--skip-fetch"     in args or os.getenv("SKIP_FETCH", "false").lower() == "true"
SKIP_ML     = "--skip-ml"        in args
TRANS_ONLY  = "--transform-only" in args


# ─────────────────────────────────────────────────────────────────────────────
# Pretty print helpers
# ─────────────────────────────────────────────────────────────────────────────

def banner(text: str) -> None:
    width = 64
    print(f"\n{Fore.CYAN}{'═' * width}{Style.RESET_ALL}")
    for line in text.split("\n"):
        print(f"  {line}")
    print(f"{Fore.CYAN}{'═' * width}{Style.RESET_ALL}")


def step_header(n: int, total: int, label: str) -> None:
    print(f"\n{Fore.BLUE}{'─' * 64}{Style.RESET_ALL}")
    print(f"  {Fore.BLUE}[{n}/{total}]{Style.RESET_ALL} {label}")
    print(f"{Fore.BLUE}{'─' * 64}{Style.RESET_ALL}")


def step_done(label: str, elapsed: float) -> None:
    print(f"\n  {Fore.GREEN}✓ {label} — {elapsed:.1f}s{Style.RESET_ALL}")
    logging.info(f"Stage complete: {label}  elapsed={elapsed:.1f}s")


def step_skip(label: str) -> None:
    print(f"  {Fore.YELLOW}⊘ Skipping: {label}{Style.RESET_ALL}")
    logging.info(f"Stage skipped: {label}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    pipeline_start = datetime.now(timezone.utc)
    run_id         = str(uuid.uuid4())
    target         = os.getenv("DB_TARGET", "local").upper()
    wall_start     = time.time()
    now_str        = pipeline_start.strftime("%Y-%m-%d %H:%M UTC")

    _setup_logging(run_id)

    banner(
        f"🇦🇪  UAE Talent Intelligence — ETL Pipeline\n"
        f"   run_id  : {run_id}\n"
        f"   Started : {now_str}\n"
        f"   Target  : {target}\n"
        f"   Flags   : SKIP_FETCH={SKIP_FETCH}  SKIP_ML={SKIP_ML}  TRANSFORM_ONLY={TRANS_ONLY}"
    )

    total_steps = 4 if not TRANS_ONLY else 2

    try:
        # ── Stage 1: Fetch ────────────────────────────────────────────────────
        step_header(1, total_steps, "Data Ingestion — JSearch + Adzuna APIs")
        t = time.time()
        if SKIP_FETCH:
            step_skip("Fetch (SKIP_FETCH=true) — reusing existing data/raw/ files")
        elif TRANS_ONLY:
            step_skip("Fetch (--transform-only)")
        else:
            from etl.fetch_jobs import run_ingestion
            run_ingestion()
            step_done("Ingestion", time.time() - t)

        # ── Stage 2: Transform ────────────────────────────────────────────────
        step_header(2, total_steps, "Transform — Clean · Enrich · Extract · Impute (Tier 1)")
        t = time.time()
        from etl.transform import run_transform
        run_transform(run_id=run_id, ingestion_timestamp=pipeline_start)
        step_done("Transform", time.time() - t)

        # ── Stage 3: Salary ML ────────────────────────────────────────────────
        step_header(3, total_steps, "Salary Model — Gradient Boosting Imputation (Tier 2)")
        t = time.time()
        if SKIP_ML:
            step_skip("Salary ML (--skip-ml)")
        else:
            from etl.salary_model import train_and_impute
            train_and_impute()
            step_done("Salary Model", time.time() - t)

        # ── Stage 4: Load ─────────────────────────────────────────────────────
        if not TRANS_ONLY:
            step_header(4, total_steps, f"Load — Staging → UPSERT to PostgreSQL ({target})")
            t = time.time()
            from extraction.load_db import run_load
            run_load(run_id=run_id, pipeline_start=pipeline_start)
            step_done("Load", time.time() - t)

        # ── Summary ───────────────────────────────────────────────────────────
        elapsed = round(time.time() - wall_start, 1)
        logging.info(f"Pipeline completed successfully  run_id={run_id}  elapsed={elapsed}s")
        banner(
            f"✅  Pipeline complete in {elapsed}s\n"
            f"   run_id    : {run_id}\n"
            f"   Target DB : {target}\n"
            f"   Next step : Open Power BI → Refresh data\n"
            f"   Sync tip  : Run python sync_db.py to mirror local → Supabase"
        )

    except Exception as exc:
        elapsed = round(time.time() - wall_start, 1)
        logging.exception(f"Pipeline FAILED  run_id={run_id}  elapsed={elapsed}s  error={exc}")
        banner(
            f"❌  Pipeline FAILED after {elapsed}s\n"
            f"   run_id  : {run_id}\n"
            f"   Error   : {exc}\n"
            f"   Logs    : logs/pipeline_{pipeline_start.strftime('%Y-%m-%d')}.log"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()