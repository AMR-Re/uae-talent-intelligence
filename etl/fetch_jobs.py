"""
fetch_jobs.py
Pulls live UAE job postings from JSearch (RapidAPI) and Adzuna.
Saves raw JSON to data/raw/ for the transform step.
"""

import os, json, time, requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

RAPIDAPI_KEY   = os.getenv("RAPIDAPI_KEY")
ADZUNA_APP_ID  = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
EXCHANGE_KEY   = os.getenv("EXCHANGE_API_KEY")

SEARCH_QUERIES = [
    "HR Manager",
    "Data Analyst",
    "Business Intelligence",
    "Recruitment Specialist",
    "People Analytics",
    "Talent Acquisition",
    "Financial Analyst",
    "Software Engineer",
    "Product Manager",
    "Marketing Manager",
    "Operations Manager",
    "Project Manager",
]

UAE_LOCATIONS = ["Dubai UAE", "Abu Dhabi UAE", "Sharjah UAE"]


# ── JSearch ──────────────────────────────────────────────────────────────────

def fetch_jsearch(query: str, location: str, num_pages: int = 3) -> list:
    url = "https://jsearch.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }
    results = []
    for page in range(1, num_pages + 1):
        try:
            resp = requests.get(url, headers=headers, params={
                "query":      f"{query} in {location}",
                "page":       str(page),
                "num_pages":  "1",
                "date_posted": "month",
            }, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            jobs = data.get("data", [])
            results.extend(jobs)
            print(f"    JSearch | {query} | {location} | page {page} → {len(jobs)} jobs")
        except Exception as e:
            print(f"    ⚠  JSearch error ({query} / {location} / page {page}): {e}")
        time.sleep(1.2)   # stay within rate limits
    return results


# ── Adzuna ───────────────────────────────────────────────────────────────────

def fetch_adzuna(query: str, pages: int = 2) -> list:
    results = []
    for page in range(1, pages + 1):
        try:
            url = (
                f"https://api.adzuna.com/v1/api/jobs/ae/search/{page}"
                f"?app_id={ADZUNA_APP_ID}"
                f"&app_key={ADZUNA_APP_KEY}"
                f"&results_per_page=50"
                f"&what={query.replace(' ', '+')}"
                f"&content-type=application/json"
            )
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            jobs = data.get("results", [])
            results.extend(jobs)
            print(f"    Adzuna  | {query} | page {page} → {len(jobs)} jobs")
        except Exception as e:
            print(f"    ⚠  Adzuna error ({query} / page {page}): {e}")
        time.sleep(0.8)
    return results


# ── Exchange rates ────────────────────────────────────────────────────────────

def fetch_exchange_rates() -> dict:
    try:
        url = f"https://v6.exchangerate-api.com/v6/{EXCHANGE_KEY}/latest/AED"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        rates = resp.json().get("conversion_rates", {})
        print(f"    Exchange rates fetched: {len(rates)} currencies")
        return rates
    except Exception as e:
        print(f"    ⚠  Exchange rate error: {e}")
        # Fallback hardcoded rates (as of 2024) so pipeline doesn't break
        return {"USD": 0.272, "GBP": 0.216, "EUR": 0.250, "AED": 1.0, "INR": 22.6}


# ── Main ──────────────────────────────────────────────────────────────────────

def run_ingestion():
    os.makedirs("data/raw", exist_ok=True)
    all_jsearch, all_adzuna = [], []

    print("\n── Fetching from JSearch ──────────────────────────────")
    for query in SEARCH_QUERIES:
        for location in UAE_LOCATIONS:
            all_jsearch.extend(fetch_jsearch(query, location))

    print("\n── Fetching from Adzuna ───────────────────────────────")
    for query in SEARCH_QUERIES:
        all_adzuna.extend(fetch_adzuna(query))

    print("\n── Fetching exchange rates ────────────────────────────")
    rates = fetch_exchange_rates()

    # Stamp with fetch timestamp
    meta = {"fetched_at": datetime.utcnow().isoformat(), "counts": {
        "jsearch": len(all_jsearch), "adzuna": len(all_adzuna)
    }}

    with open("data/raw/jsearch_raw.json", "w", encoding="utf-8") as f:
        json.dump(all_jsearch, f, indent=2, ensure_ascii=False)
    with open("data/raw/adzuna_raw.json", "w", encoding="utf-8") as f:
        json.dump(all_adzuna, f, indent=2, ensure_ascii=False)
    with open("data/raw/exchange_rates.json", "w") as f:
        json.dump(rates, f, indent=2)
    with open("data/raw/meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\n✅ Ingestion complete.")
    print(f"   JSearch : {len(all_jsearch)} records → data/raw/jsearch_raw.json")
    print(f"   Adzuna  : {len(all_adzuna)} records → data/raw/adzuna_raw.json")


if __name__ == "__main__":
    run_ingestion()