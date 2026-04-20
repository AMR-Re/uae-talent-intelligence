"""
validate_setup.py
─────────────────────────────────────────────────────────────────────────────
Pre-flight check — run this before the pipeline to verify your setup.

Checks:
  ✓ .env file exists and required keys are set
  ✓ Python dependencies installed
  ✓ PostgreSQL connection works
  ✓ data/reference/ CSV files exist
  ✓ API keys are non-empty (doesn't actually call the APIs)

Usage:
    python validate_setup.py
─────────────────────────────────────────────────────────────────────────────
"""

import os
import sys
from pathlib import Path
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)

PASS  = f"{Fore.GREEN}✓{Style.RESET_ALL}"
FAIL  = f"{Fore.RED}✗{Style.RESET_ALL}"
WARN  = f"{Fore.YELLOW}⚠{Style.RESET_ALL}"

errors   = []
warnings = []


def check(label: str, condition: bool, error_msg: str = "", warn_only: bool = False) -> bool:
    if condition:
        print(f"  {PASS}  {label}")
        return True
    else:
        symbol = WARN if warn_only else FAIL
        print(f"  {symbol}  {label}")
        if error_msg:
            print(f"        → {error_msg}")
        if warn_only:
            warnings.append(label)
        else:
            errors.append(label)
        return False


print(f"\n{Fore.CYAN}{'═' * 55}{Style.RESET_ALL}")
print(f"  UAE Talent Intelligence — Setup Validation")
print(f"{Fore.CYAN}{'═' * 55}{Style.RESET_ALL}\n")


# ── .env file ────────────────────────────────────────────────────────────────
print(f"  {Fore.BLUE}[1/5] Environment file{Style.RESET_ALL}")
env_path = Path(".env")
check(".env file exists", env_path.exists(),
      "Run: cp .env.example .env  then fill in your values")

if env_path.exists():
    from dotenv import load_dotenv
    load_dotenv()

    required_keys = ["DB_URL", "RAPIDAPI_KEY", "ADZUNA_APP_ID", "ADZUNA_APP_KEY", "EXCHANGE_API_KEY"]
    optional_keys = ["SUPABASE_DB_URL", "OPENCORPORATES_API_KEY", "GLASSDOOR_RAPIDAPI_KEY"]

    for key in required_keys:
        val = os.getenv(key, "")
        check(f"{key} is set", bool(val), f"Add {key}=<your_value> to .env")

    for key in optional_keys:
        val = os.getenv(key, "")
        check(f"{key} is set (optional)", bool(val),
              f"Optional — pipeline runs without it", warn_only=True)


# ── Python packages ───────────────────────────────────────────────────────────
print(f"\n  {Fore.BLUE}[2/5] Python dependencies{Style.RESET_ALL}")
packages = {
    "pandas":       "pandas",
    "sqlalchemy":   "sqlalchemy",
    "psycopg2":     "psycopg2",
    "dotenv":       "python-dotenv",
    "requests":     "requests",
    "numpy":        "numpy",
    "sklearn":      "scikit-learn",
    "rapidfuzz":    "rapidfuzz",
    "tenacity":     "tenacity",
    "tqdm":         "tqdm",
    "colorama":     "colorama",
}
for module, pkg in packages.items():
    try:
        __import__(module)
        check(f"{pkg}", True)
    except ImportError:
        check(f"{pkg}", False, f"Run: pip install {pkg}")


# ── Database connection ───────────────────────────────────────────────────────
print(f"\n  {Fore.BLUE}[3/5] Database connection{Style.RESET_ALL}")
try:
    from etl.db import get_engine
    from sqlalchemy import text
    engine = get_engine("local")
    with engine.connect() as conn:
        db_name = conn.execute(text("SELECT current_database()")).scalar()
    check(f"Local PostgreSQL connected ({db_name})", True)
except Exception as exc:
    check("Local PostgreSQL connected", False,
          f"Error: {exc}\n"
          "        Create DB: psql -U postgres -c \"CREATE DATABASE uae_talent;\"")

# Supabase (optional)
supabase_url = os.getenv("SUPABASE_DB_URL", "")
if supabase_url:
    try:
        engine_sb = get_engine("supabase")
        with engine_sb.connect() as conn:
            conn.execute(text("SELECT 1"))
        check("Supabase connected", True)
    except Exception as exc:
        check("Supabase connected", False, str(exc), warn_only=True)
else:
    check("Supabase configured (optional)", False,
          "Set SUPABASE_DB_URL in .env to enable cloud sync", warn_only=True)


# ── Reference files ───────────────────────────────────────────────────────────
print(f"\n  {Fore.BLUE}[4/5] Reference data files{Style.RESET_ALL}")
refs = [
    "data/reference/uae_salary_benchmarks.csv",
    "data/reference/company_profiles.csv",
]
for path in refs:
    check(f"{path}", Path(path).exists(),
          f"File missing — it should be committed to the repo")


# ── Data directories ──────────────────────────────────────────────────────────
print(f"\n  {Fore.BLUE}[5/5] Data directories{Style.RESET_ALL}")
dirs = ["data/raw", "data/processed", "data/reference"]
for d in dirs:
    Path(d).mkdir(parents=True, exist_ok=True)
    check(f"{d}/ exists", True)

raw_files = ["data/raw/jsearch_raw.json", "data/raw/adzuna_raw.json"]
for f in raw_files:
    check(f"{f} (cached raw data)", Path(f).exists(),
          "Not yet fetched — run pipeline with fetch enabled", warn_only=True)


# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\n{Fore.CYAN}{'─' * 55}{Style.RESET_ALL}")
if errors:
    print(f"\n  {Fore.RED}✗ {len(errors)} error(s) — fix before running pipeline:{Style.RESET_ALL}")
    for e in errors:
        print(f"    • {e}")
    print()
    sys.exit(1)
elif warnings:
    print(f"\n  {Fore.YELLOW}⚠ {len(warnings)} warning(s) — optional items not configured.{Style.RESET_ALL}")
    print(f"  {Fore.GREEN}✅ Core setup is valid. Ready to run:{Style.RESET_ALL}")
    print(f"     python run_pipeline.py\n")
else:
    print(f"\n  {Fore.GREEN}✅ All checks passed. Ready to run:{Style.RESET_ALL}")
    print(f"     python run_pipeline.py\n")
