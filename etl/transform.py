"""
etl/transform.py  (v5 — production additions)
─────────────────────────────────────────────────────────────────────────────
Reads raw JSON → cleans → enriches → imputes → outputs clean CSVs.

Changes in v5 (production-grade additions — no existing logic touched):

  ADD 1 — run_id + ingestion_timestamp
      run_transform() now accepts run_id and ingestion_timestamp parameters
      forwarded from run_pipeline.py. Both are written to jobs_clean.csv
      so every record is traceable to the exact pipeline run.

  ADD 2 — salary_imputed_flag
      Boolean column set to True for any row where salary_source ≠ 'reported'.
      Added after impute_salaries_peer_median() completes.

  ADD 3 — confidence_score (Tier 1)
      Numeric 0–100 score assigned per salary_source label.
      Tier 2 (salary_model.py) may further refine this for ML-imputed rows.
      Sources:
        reported             → 100
        imputed_peer_L1      → 90
        imputed_peer_L2      → 80
        imputed_peer_L3      → 70
        imputed_peer_L4      → 60
        imputed_benchmark    → 55
        imputed_model        → 75  (overwritten by Tier 2 based on CV MAE)
        imputed_model_override → 70
        imputed_unknown      → 40
        missing              → 0

  RANDOM SEED (FIX 8)
      numpy random seed fixed to 42 at module import for reproducible
      fuzzy-score tie-breaking in _benchmark_fill.

All FIX 1–7 from v4 are retained unchanged.
─────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import hashlib
import re
import uuid
import warnings
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from colorama import Fore, Style, init as colorama_init

from etl.enrich_companies import enrich_dataframe

try:
    from rapidfuzz import fuzz as _rfuzz
    _RAPIDFUZZ_AVAILABLE = True
except ImportError:
    _RAPIDFUZZ_AVAILABLE = False
    warnings.warn("rapidfuzz not installed — fuzzy benchmark matching disabled")

colorama_init(autoreset=True)

# FIX 8 — fixed random seed for reproducibility
np.random.seed(42)


# ─────────────────────────────────────────────────────────────────────────────
# Confidence score map  (ADD 3)
# ─────────────────────────────────────────────────────────────────────────────

CONFIDENCE_BY_SOURCE: dict[str, int] = {
    "reported":               100,
    "imputed_peer_L1":         90,
    "imputed_peer_L2":         80,
    "imputed_peer_L3":         70,
    "imputed_peer_L4":         60,
    "imputed_benchmark":       55,
    "imputed_model":           75,   # Tier 2 may overwrite with MAE-adjusted value
    "imputed_model_override":  70,
    "imputed_unknown":         40,
    "missing":                  0,
}


# ─────────────────────────────────────────────────────────────────────────────
# Reference data  (unchanged from v4)
# ─────────────────────────────────────────────────────────────────────────────

SKILL_TAXONOMY = {
    "Power BI", "Tableau", "Looker", "Qlik", "MicroStrategy",
    "SQL", "MySQL", "PostgreSQL", "Oracle SQL", "SQL Server",
    "Python", "R", "Excel", "VBA", "Google Sheets",
    "DAX", "Power Query", "M Language",
    "Azure", "AWS", "GCP", "Databricks", "Snowflake",
    "ETL", "Data Warehouse", "Data Lake", "Apache Spark", "Kafka",
    "dbt", "Airflow", "Power Automate",
    "SAP", "SAP SuccessFactors", "Workday", "Oracle HCM",
    "Salesforce", "ServiceNow", "HRMS", "ATS",
    "Recruitment", "Talent Acquisition", "Onboarding", "Offboarding",
    "Performance Management", "HRBP", "HR Business Partner",
    "Compensation", "Benefits", "Payroll", "Emiratisation",
    "Employee Relations", "Learning and Development", "L&D",
    "Workforce Planning", "Succession Planning", "OKR", "KPI",
    "Organisational Development", "Employee Engagement",
    "Arabic", "PMP", "CIPD", "SHRM", "CHRP",
    "Leadership", "Stakeholder Management", "Agile", "Scrum",
    "Communication", "Presentation", "Change Management",
    "Financial Modelling", "Budgeting", "Forecasting",
    "IFRS", "VAT", "Audit", "CFA", "CPA", "ACCA",
}

EMIRATE_MAP = {
    "dubai":          "Dubai",
    "abu dhabi":      "Abu Dhabi",
    "sharjah":        "Sharjah",
    "ajman":          "Ajman",
    "ras al khaimah": "Ras Al Khaimah",
    "rak":            "Ras Al Khaimah",
    "fujairah":       "Fujairah",
    "umm al quwain":  "Umm Al Quwain",
}

FREEZONE_MAP = {
    "difc": "Dubai", "dafza": "Dubai", "jlt": "Dubai", "jafza": "Dubai",
    "dmcc": "Dubai", "dso": "Dubai", "tecom": "Dubai", "marina": "Dubai",
    "silicon oasis": "Dubai", "media city": "Dubai", "internet city": "Dubai",
    "business bay": "Dubai", "downtown": "Dubai",
    "adgm": "Abu Dhabi", "masdar": "Abu Dhabi", "kizad": "Abu Dhabi",
    "khalifa city": "Abu Dhabi", "adnec": "Abu Dhabi",
    "rakia": "Ras Al Khaimah",
    "hamriyah": "Sharjah", "saif zone": "Sharjah",
}

SECTOR_KEYWORDS = {
    "Technology":  ["software", "developer", "engineer", "it ", "tech", "data",
                    "cloud", "cyber", "devops", "product manager"],
    "Finance":     ["finance", "financial", "accounting", "audit", "banking",
                    "investment", "cfo", "treasury", "credit", "risk"],
    "HR":          ["hr ", "human resource", "recruitment", "talent", "people",
                    "hrbp", "payroll", "emiratisation", "workforce"],
    "Marketing":   ["marketing", "brand", "digital", "seo", "social media",
                    "content", "cmo", "communications"],
    "Operations":  ["operations", "supply chain", "logistics", "procurement",
                    "project manager", "programme manager", "facilities"],
    "Healthcare":  ["health", "medical", "pharma", "clinical", "nurse", "doctor",
                    "hospital", "laboratory"],
    "Hospitality": ["hotel", "hospitality", "tourism", "restaurant", "f&b",
                    "food and beverage", "catering"],
    "Education":   ["education", "teaching", "teacher", "university", "school",
                    "training", "e-learning"],
    "Real Estate": ["real estate", "property", "construction", "facilities",
                    "asset management", "developer"],
    "Legal":       ["legal", "lawyer", "compliance", "regulatory", "counsel",
                    "paralegal", "attorney"],
}

SENIORITY_RULES = [
    ("C-Suite",   ["chief", r"\bceo\b", r"\bcto\b", r"\bcfo\b", r"\bcoo\b",
                   r"\bcpo\b", "president", "founder", "co-founder"]),
    ("Director+", ["vp", "vice president", "director", "head of", "svp", "evp"]),
    ("Manager",   ["manager", "lead", "principal", "senior manager", "team lead",
                   "group manager"]),
    ("Senior",    ["senior", "sr.", r"\bsr\b", "specialist", "consultant"]),
    ("Junior",    ["junior", "jr.", r"\bjr\b", "graduate", "trainee",
                   "intern", "associate", "entry level", "entry-level"]),
]

KEY_FIELDS_FOR_COMPLETENESS = [
    "salary_aed_monthly", "emirate", "employment_type",
    "company", "posted_at", "sector", "skills_count",
]

MIN_REPORTED_SALARY_ROWS  = 10
BENCHMARK_FUZZY_THRESHOLD = 65


# ─────────────────────────────────────────────────────────────────────────────
# Helpers  (unchanged from v4)
# ─────────────────────────────────────────────────────────────────────────────

def detect_emirate(city: str, description: str = "") -> tuple[str, str]:
    text = (city or "").lower()
    for key, val in EMIRATE_MAP.items():
        if key in text:
            return val, "city_field"
    for zone, emirate in FREEZONE_MAP.items():
        if zone in text:
            return emirate, "freezone"
    desc_lower = (description or "").lower()
    for key, val in EMIRATE_MAP.items():
        if key in desc_lower:
            return val, "description"
    for zone, emirate in FREEZONE_MAP.items():
        if zone in desc_lower:
            return emirate, "description_freezone"
    return "Dubai", "fallback_default"


def detect_sector(title: str, description: str = "") -> str:
    text = f"{title} {description}".lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(re.search(kw, text) for kw in keywords):
            return sector
    return "Other"


def extract_skills(text: str) -> list[str]:
    if not text:
        return []
    text_lower = text.lower()
    return sorted({skill for skill in SKILL_TAXONOMY if skill.lower() in text_lower})


def add_seniority_band(title: str) -> str:
    title_lower = (title or "").lower()
    for band, patterns in SENIORITY_RULES:
        for pattern in patterns:
            if re.search(pattern, title_lower):
                return band
    return "Mid-Level"


def compute_completeness_score(row: dict) -> int:
    score = 0
    total = len(KEY_FIELDS_FOR_COMPLETENESS)
    for field in KEY_FIELDS_FOR_COMPLETENESS:
        val = row.get(field)
        if val is None or str(val) in ("", "0", "0.0", "nan"):
            continue
        if field == "emirate" and val == "Dubai" and row.get("emirate_source") == "fallback_default":
            score += 0.5
            continue
        if field == "salary_aed_monthly" and float(val or 0) == 0:
            continue
        score += 1
    return round((score / total) * 100)


def normalize_salary_to_aed_monthly(
    salary_min, salary_max, currency: str, rates: dict
) -> float:
    """FIX 7a — NaN-safe salary normalisation (unchanged from v4)."""
    def _safe_float(v) -> float:
        if v is None:
            return 0.0
        try:
            f = float(v)
            return 0.0 if np.isnan(f) else f
        except (TypeError, ValueError):
            return 0.0

    try:
        s_min = _safe_float(salary_min)
        s_max = _safe_float(salary_max)

        if s_min == 0 and s_max == 0:
            return 0.0

        mid  = (s_min + s_max) / 2 if s_max > 0 else s_min
        rate = rates.get((currency or "").upper())

        if not rate or rate == 0:
            return 0.0

        aed_value   = mid / rate
        aed_monthly = aed_value / 12 if aed_value > 50_000 else aed_value

        if np.isnan(aed_monthly) or aed_monthly < 1_000 or aed_monthly > 250_000:
            return 0.0

        return round(aed_monthly, 0)

    except Exception:
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# salary_source initialisation  (FIX 1 + FIX 7b — unchanged from v4)
# ─────────────────────────────────────────────────────────────────────────────

def _assign_salary_source(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["salary_aed_monthly"] = (
        pd.to_numeric(df["salary_aed_monthly"], errors="coerce")
        .fillna(0.0)
    )
    df["salary_source"] = np.where(
        df["salary_aed_monthly"] > 0,
        "reported",
        "missing",
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Peer-median fill  (FIX 2 + FIX 7c — unchanged from v4)
# ─────────────────────────────────────────────────────────────────────────────

def _peer_median_fill(
    df: pd.DataFrame,
    group_cols: list[str],
    label: str,
) -> tuple[pd.DataFrame, int]:
    remaining_mask = ~(df["salary_aed_monthly"] > 0)
    if not remaining_mask.any():
        return df, 0

    reported_df = df[df["salary_aed_monthly"] > 0]
    if reported_df.empty:
        return df, 0

    medians = (
        reported_df
        .groupby(group_cols)["salary_aed_monthly"]
        .median()
        .reset_index()
        .rename(columns={"salary_aed_monthly": "_imputed_val"})
    )
    if medians.empty:
        return df, 0

    orig_idx = df[remaining_mask].index.to_numpy()

    tmp = (
        df[remaining_mask][group_cols]
        .reset_index(drop=True)
        .merge(medians, on=group_cols, how="left")
    )

    filled_bool  = (tmp["_imputed_val"].notna() & (tmp["_imputed_val"] > 0)).to_numpy()
    filled_count = int(filled_bool.sum())

    if filled_count > 0:
        fill_idx  = orig_idx[filled_bool]
        fill_vals = tmp.loc[filled_bool, "_imputed_val"].to_numpy()
        df.loc[fill_idx, "salary_aed_monthly"] = np.round(fill_vals, 0)
        df.loc[fill_idx, "salary_source"]      = label

    return df, filled_count


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark fill  (FIX 3 + FIX 7c — unchanged from v4)
# ─────────────────────────────────────────────────────────────────────────────

def _benchmark_fill(
    df: pd.DataFrame,
    benchmarks: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    still_missing = ~(df["salary_aed_monthly"] > 0)
    if not still_missing.any():
        return df, 0

    bm = benchmarks.copy()
    bm["_bm_norm_lower"] = bm["title_normalized"].str.lower().str.strip()

    missing_df = df[still_missing].copy()

    unique_pairs = (
        missing_df[["title_normalized", "title", "seniority_band"]]
        .drop_duplicates(subset=["title_normalized", "seniority_band"])
        .copy()
    )
    unique_pairs["_norm_lower"] = unique_pairs["title_normalized"].str.lower().str.strip()
    unique_pairs["_raw_lower"]  = unique_pairs["title"].str.lower().str.strip()
    unique_pairs["_val"]        = np.nan

    for i, pair in unique_pairs.iterrows():
        seniority  = pair["seniority_band"]
        same_band  = bm[bm["seniority_band"] == seniority]
        candidates = same_band if not same_band.empty else bm

        exact = candidates[candidates["_bm_norm_lower"] == pair["_norm_lower"]]
        if not exact.empty:
            unique_pairs.at[i, "_val"] = float(exact.iloc[0]["median_aed_monthly"])
            continue

        if _RAPIDFUZZ_AVAILABLE:
            scores     = candidates["_bm_norm_lower"].apply(
                lambda t: _rfuzz.token_sort_ratio(pair["_raw_lower"], t)
            )
            best_idx   = scores.idxmax()
            best_score = float(scores[best_idx])
            if best_score >= BENCHMARK_FUZZY_THRESHOLD:
                unique_pairs.at[i, "_val"] = float(
                    candidates.loc[best_idx, "median_aed_monthly"]
                )
        else:
            if not same_band.empty:
                unique_pairs.at[i, "_val"] = float(same_band["median_aed_monthly"].median())

    lookup = (
        unique_pairs[["title_normalized", "seniority_band", "_val"]]
        .dropna(subset=["_val"])
        .rename(columns={"_val": "_imputed_val"})
    )
    if lookup.empty:
        return df, 0

    orig_idx = missing_df.index.to_numpy()

    tmp = (
        missing_df[["title_normalized", "seniority_band"]]
        .reset_index(drop=True)
        .merge(lookup, on=["title_normalized", "seniority_band"], how="left")
    )

    filled_bool  = (tmp["_imputed_val"].notna() & (tmp["_imputed_val"] > 0)).to_numpy()
    filled_count = int(filled_bool.sum())

    if filled_count > 0:
        fill_idx  = orig_idx[filled_bool]
        fill_vals = tmp.loc[filled_bool, "_imputed_val"].to_numpy()
        df.loc[fill_idx, "salary_aed_monthly"] = np.round(fill_vals, 0)
        df.loc[fill_idx, "salary_source"]      = "imputed_benchmark"

    return df, filled_count


# ─────────────────────────────────────────────────────────────────────────────
# Imputation orchestrator  (FIX 4-7 — unchanged; ADD 2+3 appended after)
# ─────────────────────────────────────────────────────────────────────────────

def impute_salaries_peer_median(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["salary_aed_monthly"] = df["salary_aed_monthly"].fillna(0.0)

    has_salary     = df["salary_aed_monthly"] > 0
    reported_count = int(has_salary.sum())
    missing_count  = int((~has_salary).sum())

    print(f"    Reported salaries : {reported_count:,}")
    print(f"    Missing salaries  : {missing_count:,}")

    if reported_count < MIN_REPORTED_SALARY_ROWS:
        print(
            f"\n    {Fore.YELLOW}⚠  Only {reported_count} reported salaries "
            f"(threshold: {MIN_REPORTED_SALARY_ROWS}).\n"
            f"       Peer-median groups will be sparse — benchmark CSV "
            f"is the primary source.{Style.RESET_ALL}\n"
        )

    if missing_count == 0:
        print(f"    {Fore.GREEN}✓  No missing salaries.{Style.RESET_ALL}")
        return df

    levels = [
        (["title_normalized", "emirate", "seniority_band"], "imputed_peer_L1"),
        (["sector", "emirate", "seniority_band"],           "imputed_peer_L2"),
        (["sector", "seniority_band"],                      "imputed_peer_L3"),
        (["sector"],                                        "imputed_peer_L4"),
    ]
    for group_cols, label in levels:
        remaining = int((~(df["salary_aed_monthly"] > 0)).sum())
        if remaining == 0:
            break
        df, n = _peer_median_fill(df, group_cols, label)
        n_groups = df[df["salary_aed_monthly"] > 0].groupby(group_cols).ngroups
        print(f"    {label}: filled {n:,} rows  (peer groups: {n_groups:,})")

    benchmark_path = "data/reference/uae_salary_benchmarks.csv"
    still_missing  = int((~(df["salary_aed_monthly"] > 0)).sum())

    if still_missing > 0 and os.path.exists(benchmark_path):
        benchmarks    = pd.read_csv(benchmark_path)
        required_cols = {"title_normalized", "seniority_band", "median_aed_monthly"}
        if not required_cols.issubset(benchmarks.columns):
            missing_cols = required_cols - set(benchmarks.columns)
            print(f"    {Fore.YELLOW}⚠  Benchmark missing columns {missing_cols} — skipped{Style.RESET_ALL}")
        else:
            df, bm_n = _benchmark_fill(df, benchmarks)
            mode = (
                f"fuzzy>={BENCHMARK_FUZZY_THRESHOLD}"
                if _RAPIDFUZZ_AVAILABLE
                else "exact+band-fallback"
            )
            print(f"    imputed_benchmark : filled {bm_n:,} rows  ({mode}, {still_missing:,} attempted)")
    elif still_missing > 0:
        print(
            f"    {Fore.YELLOW}⚠  Benchmark CSV not found at {benchmark_path} — skipped.\n"
            f"       Add data/reference/uae_salary_benchmarks.csv to enable L5 "
            f"imputation.{Style.RESET_ALL}"
        )

    # FIX 4 + FIX 6: summary + consistency guard
    final_missing  = int((~(df["salary_aed_monthly"] > 0)).sum())
    filled_total   = missing_count - final_missing
    source_missing = int((df["salary_source"] == "missing").sum())

    if source_missing != final_missing:
        still_zero_mask = ~(df["salary_aed_monthly"] > 0)
        df.loc[still_zero_mask,  "salary_source"] = "missing"
        df.loc[
            ~still_zero_mask & (df["salary_source"] == "missing"),
            "salary_source",
        ] = "imputed_unknown"
        print(
            f"\n    {Fore.YELLOW}⚠  salary_source / salary_aed_monthly mismatch "
            f"({source_missing} labelled missing vs {final_missing} actually missing).\n"
            f"       Labels resynced.{Style.RESET_ALL}"
        )

    print(f"\n    Imputation summary (Tier 1):")
    print(f"      Started missing  : {missing_count:,}")
    print(f"      Total filled     : {filled_total:,}")
    print(f"      Still missing    : {final_missing:,}")
    source_counts = df["salary_source"].value_counts().to_dict()
    for src, n in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"        {src:<38}: {n:,}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# ADD 2+3 — salary_imputed_flag + confidence_score (Tier 1 assignment)
# ─────────────────────────────────────────────────────────────────────────────

def assign_imputed_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sets salary_imputed_flag and confidence_score based on salary_source.
    Called after all Tier 1 imputation is complete.
    Tier 2 (salary_model.py) may overwrite confidence_score for ML rows.
    """
    df = df.copy()
    df["salary_imputed_flag"] = df["salary_source"] != "reported"
    df["confidence_score"] = df["salary_source"].map(CONFIDENCE_BY_SOURCE).fillna(40).astype(float)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Title normalisation  (FIX 3 — expanded map, unchanged from v4)
# ─────────────────────────────────────────────────────────────────────────────

TITLE_NORMALISE_MAP = {
    r"hr business partner|hrbp":                "HR Business Partner",
    r"hr manager|human resource.* manager":      "HR Manager",
    r"hr director|human resource.* director":    "HR Director",
    r"chief people|chief hr|chro":               "CHRO",
    r"talent acqui\w+":                          "Talent Acquisition",
    r"recruitment\w*|recruiter":                 "Recruiter",
    r"people analy\w+":                          "People Analytics",
    r"payroll\w*":                               "Payroll Specialist",
    r"learning.+development|l[\.\s]?d\b":        "L&D Specialist",
    r"workforce\w+":                             "Workforce Planner",
    r"compensation.+benefit|c[\s&]+b\b":         "Compensation & Benefits",
    r"employee relation\w*":                     "Employee Relations",
    r"succession\w+":                            "Succession Planning",
    r"organisational dev\w*|org\w* dev\w*":      "Organisational Development",
    r"data analy\w+":                            "Data Analyst",
    r"data engineer\w*":                         "Data Engineer",
    r"bi developer|business intel\w*|power bi":  "BI Developer",
    r"software engi\w+|swe\b":                   "Software Engineer",
    r"product manager|pm\b":                     "Product Manager",
    r"financial analy\w+":                       "Financial Analyst",
    r"fp&a|financial planning":                  "FP&A Analyst",
    r"financial model\w*":                       "Financial Modelling Analyst",
    r"internal audit\w*":                        "Internal Auditor",
    r"marketing manager":                        "Marketing Manager",
    r"operations manager":                       "Operations Manager",
    r"project manager|pmp":                      "Project Manager",
    r"supply chain":                             "Supply Chain Manager",
    r"logistics\w*":                             "Logistics Manager",
    r"procurement\w*":                           "Procurement Manager",
}


def normalise_title(title: str) -> str:
    t = (title or "").lower().strip()
    for pattern, normalised in TITLE_NORMALISE_MAP.items():
        if re.search(pattern, t):
            return normalised
    return re.sub(r"\s+", " ", title).strip().title() if title else "Unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Source-specific cleaners  (unchanged from v4)
# ─────────────────────────────────────────────────────────────────────────────

def clean_jsearch(raw: list, rates: dict) -> pd.DataFrame:
    rows = []
    for job in raw:
        city = (job.get("job_city") or "").strip()
        desc = job.get("job_description", "")
        emirate, emirate_source = detect_emirate(city, desc)
        rows.append({
            "source":          "jsearch",
            "job_id":          job.get("job_id", ""),
            "title":           (job.get("job_title") or "").strip(),
            "company":         (job.get("employer_name") or "").strip(),
            "city":            city,
            "emirate":         emirate,
            "emirate_source":  emirate_source,
            "employment_type": (job.get("job_employment_type") or "").strip(),
            "remote":          bool(job.get("job_is_remote", False)),
            "description":     desc,
            "salary_min":      job.get("job_min_salary"),
            "salary_max":      job.get("job_max_salary"),
            "salary_currency": (job.get("job_salary_currency") or "USD").upper(),
            "posted_at":       job.get("job_posted_at_datetime_utc"),
            "apply_link":      job.get("job_apply_link", ""),
        })
    return _apply_transforms(pd.DataFrame(rows), rates)


def clean_adzuna(raw: list, rates: dict) -> pd.DataFrame:
    rows = []
    for job in raw:
        loc  = (job.get("location", {}) or {}).get("display_name", "")
        desc = job.get("description", "")
        emirate, emirate_source = detect_emirate(loc, desc)
        rows.append({
            "source":          "adzuna",
            "job_id":          f"adzuna_{job.get('id', '')}",
            "title":           (job.get("title") or "").strip(),
            "company":         ((job.get("company") or {}).get("display_name") or "").strip(),
            "city":            loc,
            "emirate":         emirate,
            "emirate_source":  emirate_source,
            "employment_type": (job.get("contract_time") or "").strip(),
            "remote":          False,
            "description":     desc,
            "salary_min":      job.get("salary_min"),
            "salary_max":      job.get("salary_max"),
            "salary_currency": "AED",
            "posted_at":       job.get("created"),
            "apply_link":      job.get("redirect_url", ""),
        })
    return _apply_transforms(pd.DataFrame(rows), rates)


def _apply_transforms(df: pd.DataFrame, rates: dict) -> pd.DataFrame:
    if df.empty:
        return df
    df["title_normalized"]   = df["title"].apply(normalise_title)
    df["sector"]             = df.apply(
        lambda r: detect_sector(r["title"], r.get("description", "")), axis=1
    )
    df["seniority_band"]     = df["title"].apply(add_seniority_band)
    df["skills"]             = df["description"].apply(extract_skills)
    df["skills_count"]       = df["skills"].apply(len)
    df["salary_aed_monthly"] = df.apply(
        lambda r: normalize_salary_to_aed_monthly(
            r["salary_min"], r["salary_max"], r["salary_currency"], rates
        ),
        axis=1,
    )
    df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce", utc=True)
    df["days_since_posted"] = (
        pd.Timestamp.now(tz="UTC") - df["posted_at"]
    ).dt.days.clip(lower=0)
    return df.drop_duplicates(subset=["job_id"])


# ─────────────────────────────────────────────────────────────────────────────
# Main runner  (ADD 1 — accepts run_id + ingestion_timestamp)
# ─────────────────────────────────────────────────────────────────────────────

def dedup_cross_source(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Removes cross-source duplicates — jobs where job_id differs but the
    real-world posting is identical (same title + company + emirate).

    Strategy:
      1. Build a content hash from normalised title + company + emirate.
      2. Where multiple rows share the same hash, keep the jsearch row
         (richer salary/metadata) and drop the rest.
      3. Return (deduped_df, n_dropped) so the caller can log the count.

    This runs AFTER job_id deduplication so it only catches genuine
    cross-source matches, not within-source page duplicates.
    """
    before = len(df)

    df = df.copy()
    df["_content_hash"] = (
        df["title_normalized"].str.lower().str.strip() + "|" +
        df["company"].str.lower().str.strip()          + "|" +
        df["emirate"].str.lower().str.strip()
    ).apply(lambda x: hashlib.md5(x.encode()).hexdigest())

    # Sort so jsearch rows come first (kept by keep="first")
    df = df.sort_values("source", ascending=True)   # 'adzuna' > 'jsearch' alphabetically → jsearch first
    df = df.sort_values("source", key=lambda s: s.map({"jsearch": 0, "adzuna": 1}).fillna(2))

    df = df.drop_duplicates(subset=["_content_hash"], keep="first")
    df = df.drop(columns=["_content_hash"])

    return df.reset_index(drop=True), before - len(df)


def run_transform(
    run_id: Optional[str] = None,
    ingestion_timestamp=None,
) -> None:
    """
    Parameters
    ----------
    run_id : str, optional
        UUID forwarded from run_pipeline.py. A new UUID is generated if not
        provided (e.g. when run_transform is called standalone for testing).
    ingestion_timestamp : datetime, optional
        UTC timestamp of pipeline start. Defaults to now if not provided.
    """
    if run_id is None:
        run_id = str(uuid.uuid4())
    if ingestion_timestamp is None:
        ingestion_timestamp = datetime.now(timezone.utc)

    os.makedirs("data/processed", exist_ok=True)

    print(f"  {Fore.CYAN}Loading raw data…{Style.RESET_ALL}")
    with open("data/raw/jsearch_raw.json",    encoding="utf-8") as f:
        jsearch_raw = json.load(f)
    with open("data/raw/adzuna_raw.json",     encoding="utf-8") as f:
        adzuna_raw  = json.load(f)
    with open("data/raw/exchange_rates.json", encoding="utf-8") as f:
        rates = json.load(f)

    print(f"  {Fore.CYAN}Cleaning JSearch…{Style.RESET_ALL}")
    df_j = clean_jsearch(jsearch_raw, rates)
    print(f"    -> {len(df_j):,} rows")

    print(f"  {Fore.CYAN}Cleaning Adzuna…{Style.RESET_ALL}")
    df_a = clean_adzuna(adzuna_raw, rates)
    print(f"    -> {len(df_a):,} rows")

    df = pd.concat([df_j, df_a], ignore_index=True).drop_duplicates(subset=["job_id"])
    before_dedup = len(df)
    df, cross_dupes = dedup_cross_source(df)
    print(f"  Combined: {before_dedup:,} rows after job_id dedup → {len(df):,} unique jobs "
          f"({cross_dupes:,} cross-source duplicates removed)")

    print(f"  {Fore.CYAN}Enriching companies…{Style.RESET_ALL}")
    df = enrich_dataframe(df)

    # FIX 1 + FIX 7b
    df = _assign_salary_source(df)

    print(f"  {Fore.CYAN}Imputing salaries (Tier 1: peer median + benchmark)…{Style.RESET_ALL}")
    df = impute_salaries_peer_median(df)

    # ADD 2+3 — imputed flag + confidence score (Tier 1 values)
    df = assign_imputed_flags(df)

    df["completeness_score"] = df.apply(
        lambda r: compute_completeness_score(r.to_dict()), axis=1
    )

    # ADD 1 — pipeline traceability columns
    df["run_id"]             = run_id
    df["ingestion_timestamp"] = ingestion_timestamp

    # ── Build skills long table ───────────────────────────────────────────────
    df_skills = (
        df[[
            "job_id", "title", "title_normalized", "company", "emirate",
            "sector", "seniority_band", "skills", "salary_aed_monthly",
            "salary_source", "posted_at",
        ]]
        .copy()
        .explode("skills")
        .dropna(subset=["skills"])
        .rename(columns={"skills": "skill"})
    )
    df_skills = df_skills[df_skills["skill"].str.strip() != ""]

    # ── Write outputs ─────────────────────────────────────────────────────────
    df_out = df.drop(columns=["skills", "description", "emirate_source"], errors="ignore")
    df_out.to_csv("data/processed/jobs_clean.csv", index=False)
    df_skills.to_csv("data/processed/skills_long.csv", index=False)

    salary_coverage = round((df["salary_aed_monthly"] > 0).mean() * 100, 1)
    meta = {
        "transformed_at":           datetime.utcnow().isoformat(),
        "run_id":                   run_id,
        "total_jobs":               len(df),
        "cross_source_dupes_removed": cross_dupes,
        "total_skill_tags":         len(df_skills),
        "salary_coverage_pct":      salary_coverage,
        "salary_source_split": df["salary_source"].value_counts().to_dict(),
        "imputed_flag_pct":    round(df["salary_imputed_flag"].mean() * 100, 1),
        "emirate_breakdown":   df["emirate"].value_counts().to_dict(),
        "sector_breakdown":    df["sector"].value_counts().to_dict(),
        "seniority_breakdown": df["seniority_band"].value_counts().to_dict(),
        "avg_completeness":    round(df["completeness_score"].mean(), 1),
        "avg_confidence":      round(df["confidence_score"].mean(), 1),
        "sources": {
            "jsearch": int((df["source"] == "jsearch").sum()),
            "adzuna":  int((df["source"] == "adzuna").sum()),
        },
    }
    with open("data/processed/transform_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\n  {Fore.GREEN}✅ Transform complete{Style.RESET_ALL}")
    print(f"     jobs_clean.csv     : {len(df_out):,} rows")
    print(f"     Cross-src dupes    : {cross_dupes:,} removed")
    print(f"     skills_long.csv    : {len(df_skills):,} rows")
    print(f"     Salary coverage    : {salary_coverage}%")
    print(f"     Avg completeness   : {meta['avg_completeness']}%")
    print(f"     Avg confidence     : {meta['avg_confidence']}")
    print(f"     Imputed flag pct   : {meta['imputed_flag_pct']}%")
    print(f"     run_id             : {run_id}")
    print(f"     Emirate split      : {meta['emirate_breakdown']}")


if __name__ == "__main__":
    run_transform()