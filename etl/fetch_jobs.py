"""
etl/fetch_jobs.py  (v2 — key rotation)
─────────────────────────────────────────────────────────────────────────────
Pulls live UAE job postings from three API sources:
  1. JSearch (RapidAPI)  — aggregates LinkedIn, Indeed, Glassdoor
  2. Adzuna              — official UAE job board API
  3. Exchange rates      — ExchangeRate-API for AED normalisation

Raw JSON saved to data/raw/ for the transform step.

KEY ROTATION (v2)
─────────────────
  Keys are loaded from RAPIDAPI_KEYS (comma-separated) in .env / GitHub
  Secrets. Each weekly pipeline run uses exactly ONE primary key, selected
  deterministically by ISO week number:

      primary_index = ISO_week_number % len(keys)

  If the primary key returns HTTP 429, exactly ONE fallback key is tried
  (the next key in the list, wrapping around). If the fallback also fails,
  the error is logged and the request fails — no further keys are tried.

  ┌─────────────────────────────────────────────────────┐
  │  Normal run  →  1 key used                          │
  │  Primary 429 →  2 keys used (primary + fallback)    │
  │  Both fail   →  logged + pipeline fails gracefully  │
  │  NEVER       →  3+ keys used in one run             │
  └─────────────────────────────────────────────────────┘

  Single-key fallback (legacy: RAPIDAPI_KEY env var) is still supported
  for local dev when no rotation list is configured.
─────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import logging
import time
from datetime import datetime, timezone, date

import requests
from dotenv import load_dotenv
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, RetryError,
)
from tqdm import tqdm
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)
load_dotenv()

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Key rotation
# ─────────────────────────────────────────────────────────────────────────────

def _load_keys() -> list[str]:
    """
    Loads RapidAPI keys from environment.

    Priority:
      1. RAPIDAPI_KEYS  — comma-separated list (rotation mode)
      2. RAPIDAPI_KEY   — single key (legacy / local dev)

    Returns a list with at least one key, or raises if none configured.
    """
    multi = os.getenv("RAPIDAPI_KEYS", "").strip()
    if multi:
        keys = [k.strip() for k in multi.split(",") if k.strip()]
        if keys:
            return keys

    single = os.getenv("RAPIDAPI_KEY", "").strip()
    if single:
        return [single]

    raise EnvironmentError(
        "No RapidAPI key configured. "
        "Set RAPIDAPI_KEYS (comma-separated) or RAPIDAPI_KEY in your .env / GitHub Secrets."
    )


def _select_keys_for_run(keys: list[str]) -> tuple[str, str | None, int]:
    """
    Deterministically selects the primary key for this week's run using the
    ISO week number. Also returns the fallback key (next in list) and the
    index used — both logged to stdout and the pipeline log file.

    Returns: (primary_key, fallback_key_or_None, primary_index)
    """
    iso_week = date.today().isocalendar().week
    primary_idx  = iso_week % len(keys)
    fallback_idx = (primary_idx + 1) % len(keys)

    primary  = keys[primary_idx]
    fallback = keys[fallback_idx] if len(keys) > 1 else None

    msg = (
        f"Key rotation — ISO week {iso_week}, "
        f"{len(keys)} key(s) configured. "
        f"Primary index: {primary_idx}"
        + (f", fallback index: {fallback_idx}" if fallback else " (single-key mode, no fallback)")
    )
    print(f"  {Fore.CYAN}🔑 {msg}{Style.RESET_ALL}")
    logger.info(msg)

    return primary, fallback, primary_idx


# ─────────────────────────────────────────────────────────────────────────────
# Credentials
# ─────────────────────────────────────────────────────────────────────────────

ADZUNA_APP_ID  = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
EXCHANGE_KEY   = os.getenv("EXCHANGE_API_KEY")
SKIP_FETCH     = os.getenv("SKIP_FETCH", "false").lower() == "true"
JSEARCH_PAGES  = int(os.getenv("JSEARCH_PAGES", "3"))
ADZUNA_PAGES   = int(os.getenv("ADZUNA_PAGES", "2"))

# Keys loaded once at module level — raises early if misconfigured
_ALL_KEYS                         = _load_keys()
_PRIMARY_KEY, _FALLBACK_KEY, _KEY_IDX = _select_keys_for_run(_ALL_KEYS)

# Track whether we have already escalated to the fallback this run.
# Once set, no further key escalation is allowed.
_fallback_used: bool = False


# ─────────────────────────────────────────────────────────────────────────────
# Search configuration
# ─────────────────────────────────────────────────────────────────────────────

SEARCH_QUERIES = [
    # HR & People
    "HR Manager",
    "HRBP HR Business Partner",
    "Talent Acquisition Specialist",
    "Recruitment Specialist",
    "People Analytics",
    "Compensation and Benefits",
    "Payroll Manager",
    "Learning and Development Manager",
    "Workforce Planning",
    "Employee Relations",
    # Analytics & BI
    "Data Analyst",
    "Business Intelligence Analyst",
    "Power BI Developer",
    "Financial Analyst",
    "People Analytics Manager",
    # Tech
    "Software Engineer",
    "Data Engineer",
    "Product Manager",
    # Business
    "Operations Manager",
    "Project Manager",
    "Marketing Manager",
]

UAE_LOCATIONS = [
    "Dubai UAE",
    "Abu Dhabi UAE",
    "Sharjah UAE",
]


# ─────────────────────────────────────────────────────────────────────────────
# Retry decorator (network errors only — 429 is handled separately)
# ─────────────────────────────────────────────────────────────────────────────

def _make_retry():
    return retry(
        retry=retry_if_exception_type(requests.exceptions.ConnectionError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# JSearch — with key rotation
# ─────────────────────────────────────────────────────────────────────────────

def _jsearch_request(query: str, location: str, page: int, api_key: str) -> list:
    """Single HTTP call to JSearch with the supplied key. Raises on error."""
    resp = requests.get(
        "https://jsearch.p.rapidapi.com/search",
        headers={
            "X-RapidAPI-Key":  api_key,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
        },
        params={
            "query":       f"{query} in {location}",
            "page":        str(page),
            "num_pages":   "1",
            "date_posted": "month",
        },
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def _jsearch_page(query: str, location: str, page: int) -> list:
    """
    Fetches one page from JSearch with controlled key rotation.

    Flow:
      1. Try primary key.
      2. If HTTP 429 and a fallback key exists and hasn't been used yet
         → log, switch to fallback, retry ONCE.
      3. If fallback also fails 429, or no fallback → log + raise.
    """
    global _fallback_used

    # ── Attempt with primary key ──────────────────────────────────────────────
    try:
        return _jsearch_request(query, location, page, _PRIMARY_KEY)

    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None

        if status != 429:
            raise  # non-quota error — re-raise immediately

        # ── Primary key rate-limited ──────────────────────────────────────────
        msg = f"Primary key (index {_KEY_IDX}) hit 429 on {query}/{location}/p{page}"
        print(f"  {Fore.YELLOW}⚠  {msg}{Style.RESET_ALL}")
        logger.warning(msg)

        if _FALLBACK_KEY is None:
            err = "No fallback key available (only one key configured). Failing."
            print(f"  {Fore.RED}✗  {err}{Style.RESET_ALL}")
            logger.error(err)
            raise

        if _fallback_used:
            err = (
                "Fallback key already used this run. "
                "Quota protection: refusing to try additional keys."
            )
            print(f"  {Fore.RED}✗  {err}{Style.RESET_ALL}")
            logger.error(err)
            raise

        # ── Try fallback key — exactly once ──────────────────────────────────
        _fallback_used = True
        fallback_idx   = (_KEY_IDX + 1) % len(_ALL_KEYS)
        msg2 = f"Switching to fallback key (index {fallback_idx}) for this request"
        print(f"  {Fore.YELLOW}⚠  {msg2}{Style.RESET_ALL}")
        logger.warning(msg2)

        try:
            return _jsearch_request(query, location, page, _FALLBACK_KEY)

        except requests.exceptions.HTTPError as exc2:
            status2 = exc2.response.status_code if exc2.response is not None else None
            if status2 == 429:
                err = (
                    f"Fallback key (index {fallback_idx}) also hit 429. "
                    "Both keys exhausted for this run. Pipeline will fail gracefully."
                )
            else:
                err = f"Fallback key failed with HTTP {status2}: {exc2}"
            print(f"  {Fore.RED}✗  {err}{Style.RESET_ALL}")
            logger.error(err)
            raise


def fetch_jsearch(query: str, location: str) -> list:
    results = []
    for page in range(1, JSEARCH_PAGES + 1):
        try:
            jobs = _jsearch_page(query, location, page)
            results.extend(jobs)
        except Exception as exc:
            print(f"    {Fore.YELLOW}⚠{Style.RESET_ALL} JSearch {query}/{location}/p{page}: {exc}")
            logger.warning(f"JSearch failed: {query}/{location}/p{page}: {exc}")
        time.sleep(1.2)   # respect rate limit
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Adzuna  (unchanged — uses its own app_id/app_key, not RapidAPI)
# ─────────────────────────────────────────────────────────────────────────────

@_make_retry()
def _adzuna_page(query: str, page: int) -> list:
    """Fetch a single page from Adzuna UAE."""
    resp = requests.get(
        f"https://api.adzuna.com/v1/api/jobs/ae/search/{page}",
        params={
            "app_id":           ADZUNA_APP_ID,
            "app_key":          ADZUNA_APP_KEY,
            "results_per_page": 50,
            "what":             query,
            "content-type":     "application/json",
        },
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def fetch_adzuna(query: str) -> list:
    results = []
    for page in range(1, ADZUNA_PAGES + 1):
        try:
            jobs = _adzuna_page(query, page)
            results.extend(jobs)
        except Exception as exc:
            print(f"    {Fore.YELLOW}⚠{Style.RESET_ALL} Adzuna {query}/p{page}: {exc}")
            logger.warning(f"Adzuna failed: {query}/p{page}: {exc}")
        time.sleep(0.8)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Exchange rates
# ─────────────────────────────────────────────────────────────────────────────

def fetch_exchange_rates() -> dict:
    """
    Fetches AED-based exchange rates from ExchangeRate-API.
    Falls back to hardcoded Dec-2024 rates if the API call fails.
    Rates mean: 1 AED = X foreign currency units.
    """
    try:
        resp = requests.get(
            f"https://v6.exchangerate-api.com/v6/{EXCHANGE_KEY}/latest/AED",
            timeout=10,
        )
        resp.raise_for_status()
        rates = resp.json().get("conversion_rates", {})
        print(f"    {Fore.GREEN}✓{Style.RESET_ALL} Exchange rates fetched ({len(rates)} currencies)")
        return rates
    except Exception as exc:
        print(f"    {Fore.YELLOW}⚠{Style.RESET_ALL} Exchange API failed ({exc}) — using fallback rates")
        logger.warning(f"Exchange rate API failed: {exc} — using hardcoded fallback")
        return {
            "AED": 1.000,
            "USD": 0.272,
            "GBP": 0.216,
            "EUR": 0.250,
            "INR": 22.60,
            "PKR": 75.80,
            "PHP": 15.90,
            "EGP": 13.20,
            "SAR": 1.020,
            "KWD": 0.084,
            "QAR": 0.991,
            "BHD": 0.103,
            "OMR": 0.105,
            "JOD": 0.193,
            "LBP": 486.0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Main ingestion runner
# ─────────────────────────────────────────────────────────────────────────────

def run_ingestion():
    os.makedirs("data/raw", exist_ok=True)

    if SKIP_FETCH:
        print(f"  {Fore.CYAN}SKIP_FETCH=true{Style.RESET_ALL} — reusing existing raw files.")
        return

    all_jsearch: list = []
    all_adzuna:  list = []

    # JSearch — query × location matrix
    print(f"\n  {Fore.CYAN}── JSearch ──────────────────────────────────────────────{Style.RESET_ALL}")
    combos = [(q, loc) for q in SEARCH_QUERIES for loc in UAE_LOCATIONS]
    for query, location in tqdm(combos, desc="  JSearch", unit="combo"):
        jobs = fetch_jsearch(query, location)
        all_jsearch.extend(jobs)

    # Adzuna — query only (UAE country fixed in API call)
    print(f"\n  {Fore.CYAN}── Adzuna ───────────────────────────────────────────────{Style.RESET_ALL}")
    for query in tqdm(SEARCH_QUERIES, desc="  Adzuna", unit="query"):
        jobs = fetch_adzuna(query)
        all_adzuna.extend(jobs)

    # Exchange rates
    print(f"\n  {Fore.CYAN}── Exchange rates ───────────────────────────────────────{Style.RESET_ALL}")
    rates = fetch_exchange_rates()

    # Metadata — include key rotation observability
    fetched_at = datetime.now(timezone.utc).isoformat()
    meta = {
        "fetched_at": fetched_at,
        "key_rotation": {
            "iso_week":         date.today().isocalendar().week,
            "total_keys":       len(_ALL_KEYS),
            "primary_key_index": _KEY_IDX,
            "fallback_used":    _fallback_used,
        },
        "counts": {
            "jsearch_raw": len(all_jsearch),
            "adzuna_raw":  len(all_adzuna),
        },
        "queries":   SEARCH_QUERIES,
        "locations": UAE_LOCATIONS,
    }

    # Persist
    _write("data/raw/jsearch_raw.json",    all_jsearch)
    _write("data/raw/adzuna_raw.json",     all_adzuna)
    _write("data/raw/exchange_rates.json", rates)
    _write("data/raw/meta.json",           meta)

    fallback_note = f"  (⚠ fallback key used)" if _fallback_used else ""
    print(f"\n  {Fore.GREEN}✅ Ingestion complete{Style.RESET_ALL}{fallback_note}")
    print(f"     JSearch  : {len(all_jsearch):,} raw records")
    print(f"     Adzuna   : {len(all_adzuna):,} raw records")
    print(f"     Key used : index {_KEY_IDX} of {len(_ALL_KEYS)}"
          + (f" + fallback index {(_KEY_IDX + 1) % len(_ALL_KEYS)}" if _fallback_used else ""))


def _write(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False, default=str)


if __name__ == "__main__":
    run_ingestion()