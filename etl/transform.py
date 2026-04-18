"""
transform.py
Reads raw JSON from data/raw/, cleans it, extracts skills,
normalises salaries to AED, and outputs two CSVs to data/processed/.
"""

import os, json, re
import pandas as pd
from datetime import datetime

SKILL_TAXONOMY = {
    # BI & Analytics
    "Power BI", "Tableau", "Looker", "Qlik", "MicroStrategy",
    "SQL", "MySQL", "PostgreSQL", "Oracle SQL", "SQL Server",
    "Python", "R", "Excel", "VBA", "Google Sheets",
    "DAX", "Power Query", "M Language",
    # Cloud & Data Engineering
    "Azure", "AWS", "GCP", "Databricks", "Snowflake",
    "ETL", "Data Warehouse", "Data Lake", "Apache Spark", "Kafka",
    "dbt", "Airflow", "Power Automate",
    # Enterprise Systems
    "SAP", "SAP SuccessFactors", "Workday", "Oracle HCM",
    "Salesforce", "ServiceNow", "HRMS", "ATS",
    # HR-Specific
    "Recruitment", "Talent Acquisition", "Onboarding", "Offboarding",
    "Performance Management", "HRBP", "HR Business Partner",
    "Compensation", "Benefits", "Payroll", "Emiratisation",
    "Employee Relations", "Learning and Development", "L&D",
    "Workforce Planning", "Succession Planning", "OKR", "KPI",
    "Organisational Development", "Employee Engagement",
    # Credentials & Soft Skills (UAE market)
    "Arabic", "PMP", "CIPD", "SHRM", "CHRP",
    "Leadership", "Stakeholder Management", "Agile", "Scrum",
    "Communication", "Presentation", "Change Management",
    # Finance
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

SECTOR_KEYWORDS = {
    "Technology":    ["software", "developer", "engineer", "it ", "tech", "data", "cloud", "cyber"],
    "Finance":       ["finance", "financial", "accounting", "audit", "banking", "investment", "cfo"],
    "HR":            ["hr ", "human resource", "recruitment", "talent", "people", "hrbp", "payroll"],
    "Marketing":     ["marketing", "brand", "digital", "seo", "social media", "content", "cmo"],
    "Operations":    ["operations", "supply chain", "logistics", "procurement", "project manager"],
    "Healthcare":    ["health", "medical", "pharma", "clinical", "nurse", "doctor"],
    "Hospitality":   ["hotel", "hospitality", "tourism", "restaurant", "f&b"],
}


def detect_emirate(city: str) -> str:
    city = (city or "").lower()
    for key, val in EMIRATE_MAP.items():
        if key in city:
            return val
    return "Other UAE"


def detect_sector(title: str, description: str = "") -> str:
    text = f"{title} {description}".lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return sector
    return "Other"


def extract_skills(text: str) -> list:
    if not text:
        return []
    text_lower = text.lower()
    return sorted({skill for skill in SKILL_TAXONOMY if skill.lower() in text_lower})


def normalize_salary_to_aed_monthly(
    salary_min, salary_max, currency: str, rates: dict
) -> float:
    """Convert any salary range → AED monthly midpoint."""
    try:
        s_min = float(salary_min) if salary_min else 0.0
        s_max = float(salary_max) if salary_max else 0.0
        if s_min == 0 and s_max == 0:
            return 0.0
        mid = (s_min + s_max) / 2 if s_max > 0 else s_min

        # rates dict: how many AED per 1 unit of foreign currency
        # rates are stored relative to AED (1 AED = X foreign)
        # so to convert foreign → AED: amount / rate[foreign]
        rate = rates.get(currency.upper(), None)
        if not rate or rate == 0:
            return 0.0

        aed_annual = mid / rate
        # If the value looks monthly already (< 5000 USD), don't divide by 12
        # Heuristic: if salary > 200,000 AED it's probably annual
        aed_monthly = aed_annual / 12 if aed_annual > 50000 else aed_annual
        return round(aed_monthly, 0)
    except Exception:
        return 0.0


def clean_jsearch(raw: list, rates: dict) -> pd.DataFrame:
    rows = []
    for job in raw:
        rows.append({
            "source":          "jsearch",
            "job_id":          job.get("job_id", ""),
            "title":           (job.get("job_title") or "").strip(),
            "company":         (job.get("employer_name") or "").strip(),
            "city":            (job.get("job_city") or "").strip(),
            "emirate":         detect_emirate(job.get("job_city", "")),
            "employment_type": (job.get("job_employment_type") or "").strip(),
            "remote":          bool(job.get("job_is_remote", False)),
            "description":     job.get("job_description", ""),
            "salary_min":      job.get("job_min_salary"),
            "salary_max":      job.get("job_max_salary"),
            "salary_currency": (job.get("job_salary_currency") or "USD").upper(),
            "posted_at":       job.get("job_posted_at_datetime_utc"),
            "apply_link":      job.get("job_apply_link", ""),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["sector"]             = df.apply(lambda r: detect_sector(r["title"], r["description"]), axis=1)
    df["skills"]             = df["description"].apply(extract_skills)
    df["skills_count"]       = df["skills"].apply(len)
    df["salary_aed_monthly"] = df.apply(
        lambda r: normalize_salary_to_aed_monthly(
            r["salary_min"], r["salary_max"], r["salary_currency"], rates
        ), axis=1
    )
    df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce")
    return df.drop_duplicates(subset=["job_id"])


def clean_adzuna(raw: list, rates: dict) -> pd.DataFrame:
    rows = []
    for job in raw:
        loc = job.get("location", {}).get("display_name", "")
        rows.append({
            "source":          "adzuna",
            "job_id":          f"adzuna_{job.get('id', '')}",
            "title":           (job.get("title") or "").strip(),
            "company":         (job.get("company", {}).get("display_name") or "").strip(),
            "city":            loc,
            "emirate":         detect_emirate(loc),
            "employment_type": (job.get("contract_time") or "").strip(),
            "remote":          False,
            "description":     job.get("description", ""),
            "salary_min":      job.get("salary_min"),
            "salary_max":      job.get("salary_max"),
            "salary_currency": "GBP",    # Adzuna UAE returns GBP; we normalize
            "posted_at":       job.get("created"),
            "apply_link":      job.get("redirect_url", ""),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["sector"]             = df.apply(lambda r: detect_sector(r["title"], r["description"]), axis=1)
    df["skills"]             = df["description"].apply(extract_skills)
    df["skills_count"]       = df["skills"].apply(len)
    df["salary_aed_monthly"] = df.apply(
        lambda r: normalize_salary_to_aed_monthly(
            r["salary_min"], r["salary_max"], r["salary_currency"], rates
        ), axis=1
    )
    df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce")
    return df.drop_duplicates(subset=["job_id"])


def run_transform():
    os.makedirs("data/processed", exist_ok=True)

    with open("data/raw/jsearch_raw.json", encoding="utf-8") as f:
        jsearch_raw = json.load(f)
    with open("data/raw/adzuna_raw.json", encoding="utf-8") as f:
        adzuna_raw = json.load(f)
    with open("data/raw/exchange_rates.json") as f:
        rates = json.load(f)

    print("  Cleaning JSearch data...")
    df_j = clean_jsearch(jsearch_raw, rates)
    print(f"    → {len(df_j)} rows")

    print("  Cleaning Adzuna data...")
    df_a = clean_adzuna(adzuna_raw, rates)
    print(f"    → {len(df_a)} rows")

    df_all = pd.concat([df_j, df_a], ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["job_id"])
    print(f"  Combined total: {len(df_all)} unique jobs")

    # Explode skills into long-form table (one row per skill per job)
    df_skills = (
        df_all[["job_id", "title", "company", "emirate", "sector", "skills", "posted_at"]]
        .copy()
        .explode("skills")
        .dropna(subset=["skills"])
        .rename(columns={"skills": "skill"})
    )

    # Save — drop list column before CSV
    df_jobs_out = df_all.drop(columns=["skills", "description"])
    df_jobs_out.to_csv("data/processed/jobs_clean.csv", index=False)
    df_skills.to_csv("data/processed/skills_long.csv", index=False)

    # Save metadata
    meta = {
        "transformed_at": datetime.utcnow().isoformat(),
        "total_jobs": len(df_all),
        "total_skill_tags": len(df_skills),
        "emirate_breakdown": df_all["emirate"].value_counts().to_dict(),
        "sector_breakdown":  df_all["sector"].value_counts().to_dict(),
    }
    with open("data/processed/transform_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\n✅ Transform complete.")
    print(f"   jobs_clean.csv  → {len(df_jobs_out)} rows")
    print(f"   skills_long.csv → {len(df_skills)} rows")
    print(f"   Emirate split   : {meta['emirate_breakdown']}")


if __name__ == "__main__":
    run_transform()