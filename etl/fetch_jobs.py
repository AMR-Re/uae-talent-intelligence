"""
etl/fetch_jobs.py
─────────────────────────────────────────────────────────────────────────────
Pulls live UAE job postings from three API sources:
  1. JSearch (RapidAPI)  — aggregates LinkedIn, Indeed, Glassdoor
  2. Adzuna              — official UAE job board API
  3. Exchange rates      — ExchangeRate-API for AED normalisation

Raw JSON saved to data/raw/ for the transform step.
Set SKIP_FETCH=true in .env to reuse existing raw files (faster dev loop).
─────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)
load_dotenv()

# ── Credentials ───────────────────────────────────────────────────────────────
RAPIDAPI_KEY   = os.getenv("RAPIDAPI_KEY")
ADZUNA_APP_ID  = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
EXCHANGE_KEY   = os.getenv("EXCHANGE_API_KEY")
SKIP_FETCH     = os.getenv("SKIP_FETCH", "false").lower() == "true"
JSEARCH_PAGES  = int(os.getenv("JSEARCH_PAGES", "3"))
ADZUNA_PAGES   = int(os.getenv("ADZUNA_PAGES", "2"))

# ── Search configuration ──────────────────────────────────────────────────────
# Roles most relevant to UAE HR, Analytics, and Tech job market
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

# ── Retry decorator ────────────────────────────────────────────────────────────
def make_retry():
    return retry(
        retry=retry_if_exception_type((requests.exceptions.RequestException,)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )


# ── JSearch ───────────────────────────────────────────────────────────────────

@make_retry()
def _jsearch_page(query: str, location: str, page: int) -> list:
    """Fetch a single page from JSearch."""
    resp = requests.get(
        "https://jsearch.p.rapidapi.com/search",
        headers={
            "X-RapidAPI-Key":  RAPIDAPI_KEY,
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


def fetch_jsearch(query: str, location: str) -> list:
    results = []
    for page in range(1, JSEARCH_PAGES + 1):
        try:
            jobs = _jsearch_page(query, location, page)
            results.extend(jobs)
        except Exception as exc:
            print(f"    {Fore.YELLOW}⚠{Style.RESET_ALL} JSearch {query}/{location}/p{page}: {exc}")
        time.sleep(1.2)   # respect rate limit
    return results


# ── Adzuna ────────────────────────────────────────────────────────────────────

@make_retry()
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
        time.sleep(0.8)
    return results


# ── Exchange rates ────────────────────────────────────────────────────────────

def fetch_exchange_rates() -> dict:
    """
    Fetches AED-based exchange rates from ExchangeRate-API.
    Falls back to hardcoded 2024 rates if the API call fails.
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
        # Fallback: hand-verified approximate rates, Dec 2024
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


# ── Main ingestion runner ─────────────────────────────────────────────────────

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

    # Metadata
    fetched_at = datetime.now(timezone.utc).isoformat()
    meta = {
        "fetched_at": fetched_at,
        "counts": {
            "jsearch_raw": len(all_jsearch),
            "adzuna_raw":  len(all_adzuna),
        },
        "queries":   SEARCH_QUERIES,
        "locations": UAE_LOCATIONS,
    }

    # Persist
    _write("data/raw/jsearch_raw.json",  all_jsearch)
    _write("data/raw/adzuna_raw.json",   all_adzuna)
    _write("data/raw/exchange_rates.json", rates)
    _write("data/raw/meta.json",         meta)

    print(f"\n  {Fore.GREEN}✅ Ingestion complete{Style.RESET_ALL}")
    print(f"     JSearch : {len(all_jsearch):,} raw records")
    print(f"     Adzuna  : {len(all_adzuna):,} raw records")


def _write(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False, default=str)


if __name__ == "__main__":
    run_ingestion() 