"""
etl/enrich_companies.py
─────────────────────────────────────────────────────────────────────────────
Enriches the raw company name field with:
  • company_size_band   — headcount bracket (1-50 / 51-200 / 201-1000 / 1000+)
  • is_mnc              — True if identifiable as a multinational corporation
  • company_hq_country  — HQ country (UAE / India / US / UK / etc.)
  • company_industry    — finer-grained than our sector classification
  • glassdoor_rating    — 1–5 (if Glassdoor key available)

Sources used (in cascade — stops at first hit):
  1. Local reference table  data/reference/company_profiles.csv  (hand-curated)
  2. OpenCorporates API     (if OPENCORPORATES_API_KEY is set)
  3. Glassdoor via RapidAPI (if GLASSDOOR_RAPIDAPI_KEY is set)
  4. Heuristic fallbacks    (pattern matching on company name)

The function is called once per unique company name during transform.
Results are cached in data/reference/company_cache.json so the pipeline
doesn't re-hit APIs on every run.
─────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import time
from pathlib import Path

import requests
import pandas as pd
from rapidfuzz import fuzz
from dotenv import load_dotenv
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)
load_dotenv()

OC_KEY         = os.getenv("OPENCORPORATES_API_KEY", "")
GD_KEY         = os.getenv("GLASSDOOR_RAPIDAPI_KEY", "")
CACHE_PATH     = Path("data/reference/company_cache.json")
PROFILES_PATH  = Path("data/reference/company_profiles.csv")

# ── Known MNC patterns ────────────────────────────────────────────────────────
# Companies whose name contains any of these strings are flagged as MNC
MNC_PATTERNS = [
    "accenture", "deloitte", "pwc", "kpmg", "ey ", "ernst & young",
    "mckinsey", "bcg", "bain", "oliver wyman", "roland berger",
    "amazon", "google", "microsoft", "meta", "apple", "ibm", "oracle",
    "sap", "salesforce", "servicenow", "workday",
    "hsbc", "standard chartered", "citibank", "jpmorgan", "goldman sachs",
    "emirates", "etihad", "air arabia",
    "mubadala", "adnoc", "taqa", "dp world",
    "nestle", "unilever", "p&g", "procter", "johnson & johnson",
    "siemens", "ge ", "honeywell", "3m",
    "robert half", "hays", "michael page", "manpower", "adecco",
]

# ── Heuristic company size brackets ──────────────────────────────────────────
# Based on company name keywords — rough but useful when no API data
SIZE_HEURISTICS = {
    "1000+": [
        "group", "holdings", "international", "global", "worldwide",
        "corporation", "corp", "plc", "incorporated", "inc.",
    ],
    "201-1000": [
        "limited", "ltd", "llc", "fze", "fzco", "pjsc",
        "consulting", "services", "solutions",
    ],
    "51-200": [
        "boutique", "advisory", "studio", "agency", "partners",
    ],
    "1-50": [
        "startup", "ventures", "labs", "freelance",
    ],
}

# ── Company HQ country patterns ───────────────────────────────────────────────
HQ_PATTERNS = {
    "UAE":   ["emirati", "emaar", "damac", "aldar", "du ", "etisalat", "e&", "adnoc",
              "mubadala", "tecom", "taqa", "dewa", "sewa", "addc", "enoc", "enbd",
              "fab ", "mashreq", "adcb", "emiratisation", "uae"],
    "India": ["tata", "infosys", "wipro", "hcl", "tech mahindra", "cognizant"],
    "US":    ["inc.", "llc", "corp", "amazon", "google", "microsoft", "meta"],
    "UK":    ["plc", "ltd", "llp", "hays", "hsbc", "barclays", "bp "],
}


# ── Cache management ──────────────────────────────────────────────────────────

def _load_cache() -> dict:
    if CACHE_PATH.exists():
        with open(CACHE_PATH, encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2, ensure_ascii=False)


# ── Reference profiles ────────────────────────────────────────────────────────

def _load_profiles() -> pd.DataFrame:
    """Loads hand-curated company_profiles.csv if it exists."""
    if PROFILES_PATH.exists():
        return pd.read_csv(PROFILES_PATH)
    # Return empty frame with expected columns
    return pd.DataFrame(columns=[
        "company_name", "company_size_band", "is_mnc",
        "company_hq_country", "company_industry", "glassdoor_rating",
    ])


# ── Heuristic enrichment (no API) ─────────────────────────────────────────────

def _heuristic_profile(company: str) -> dict:
    name_lower = company.lower()

    # MNC detection
    is_mnc = any(p in name_lower for p in MNC_PATTERNS)

    # Size band
    size_band = "Unknown"
    for band, keywords in SIZE_HEURISTICS.items():
        if any(kw in name_lower for kw in keywords):
            size_band = band
            break
    if is_mnc and size_band == "Unknown":
        size_band = "1000+"

    # HQ country
    hq = "Unknown"
    for country, patterns in HQ_PATTERNS.items():
        if any(p in name_lower for p in patterns):
            hq = country
            break

    return {
        "company_size_band":  size_band,
        "is_mnc":             is_mnc,
        "company_hq_country": hq,
        "company_industry":   "Unknown",
        "glassdoor_rating":   None,
        "enrichment_source":  "heuristic",
    }


# ── OpenCorporates lookup ─────────────────────────────────────────────────────

def _opencorporates_lookup(company: str) -> dict | None:
    if not OC_KEY:
        return None
    try:
        resp = requests.get(
            "https://api.opencorporates.com/v0.4/companies/search",
            params={
                "q":               company,
                "jurisdiction_code": "ae",
                "api_token":       OC_KEY,
            },
            timeout=10,
        )
        resp.raise_for_status()
        companies = resp.json().get("results", {}).get("companies", [])
        if not companies:
            return None
        top = companies[0]["company"]
        return {
            "company_size_band":  "Unknown",   # OC doesn't provide headcount
            "is_mnc":             top.get("registered_address", {}).get("country") != "AE",
            "company_hq_country": top.get("registered_address", {}).get("country", "Unknown"),
            "company_industry":   top.get("industry_codes", [{}])[0].get("description", "Unknown")
                                  if top.get("industry_codes") else "Unknown",
            "glassdoor_rating":   None,
            "enrichment_source":  "opencorporates",
        }
    except Exception:
        return None


# ── Main enrichment function ──────────────────────────────────────────────────

_cache = _load_cache()
_profiles = _load_profiles()


def enrich_company(company: str) -> dict:
    """
    Returns enrichment dict for a single company name.
    Cascade: cache → profiles CSV → OpenCorporates → heuristics.
    """
    if not company or not company.strip():
        return _empty_profile()

    company = company.strip()
    cache_key = company.lower()

    # 1. Cache hit
    if cache_key in _cache:
        return _cache[cache_key]

    # 2. Profiles CSV — fuzzy match on company_name column
    if not _profiles.empty:
        scores = _profiles["company_name"].apply(
            lambda n: fuzz.token_sort_ratio(company.lower(), str(n).lower())
        )
        best_idx = scores.idxmax()
        if scores[best_idx] >= 85:
            row = _profiles.iloc[best_idx]
            profile = {
                "company_size_band":  row.get("company_size_band", "Unknown"),
                "is_mnc":             bool(row.get("is_mnc", False)),
                "company_hq_country": row.get("company_hq_country", "Unknown"),
                "company_industry":   row.get("company_industry", "Unknown"),
                "glassdoor_rating":   row.get("glassdoor_rating"),
                "enrichment_source":  "profiles_csv",
            }
            _cache[cache_key] = profile
            return profile

    # 3. OpenCorporates API
    oc_profile = _opencorporates_lookup(company)
    if oc_profile:
        # Merge with heuristics for fields OC doesn't cover
        heuristic = _heuristic_profile(company)
        profile = {**heuristic, **{k: v for k, v in oc_profile.items() if v != "Unknown"}}
        profile["enrichment_source"] = "opencorporates+heuristic"
        _cache[cache_key] = profile
        _save_cache(_cache)
        time.sleep(0.5)   # OpenCorporates rate limit
        return profile

    # 4. Heuristic fallback
    profile = _heuristic_profile(company)
    _cache[cache_key] = profile
    return profile


def _empty_profile() -> dict:
    return {
        "company_size_band":  "Unknown",
        "is_mnc":             False,
        "company_hq_country": "Unknown",
        "company_industry":   "Unknown",
        "glassdoor_rating":   None,
        "enrichment_source":  "empty",
    }


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches an entire DataFrame in-place.
    Expects a 'company' column. Adds enrichment columns.
    Persists cache after completion.
    """
    unique_companies = df["company"].dropna().unique()
    print(f"    Enriching {len(unique_companies)} unique companies...")

    profiles = {}
    for company in unique_companies:
        profiles[company] = enrich_company(company)

    _save_cache(_cache)

    # Map enrichment fields back to DataFrame
    enrichment_cols = [
        "company_size_band", "is_mnc",
        "company_hq_country", "company_industry",
        "glassdoor_rating", "enrichment_source",
    ]
    enrichment_df = pd.DataFrame.from_dict(profiles, orient="index")
    enrichment_df.index.name = "company"
    enrichment_df = enrichment_df.reset_index()

    df = df.merge(enrichment_df, on="company", how="left")
    missing = [c for c in enrichment_cols if c not in df.columns]
    for col in missing:
        df[col] = "Unknown"

    return df


if __name__ == "__main__":
    # Quick test
    test_companies = [
        "Deloitte Middle East",
        "ADNOC Distribution",
        "Noon E-Commerce LLC",
        "XYZ Small Startup FZE",
        "",
    ]
    print("\n── Company Enrichment Test ─────────────────────────────")
    for c in test_companies:
        result = enrich_company(c)
        print(f"\n  {c or '(empty)'}")
        for k, v in result.items():
            print(f"    {k}: {v}")