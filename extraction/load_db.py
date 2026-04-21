"""
etl/load_db.py  (v2 — production-grade)
─────────────────────────────────────────────────────────────────────────────
Reads clean CSVs → validates → stages → DQ-checks → merges to production.

Architecture:
  Stage A  validate_dataframe()     — type checks, required columns, nulls
  Stage B  load_to_staging()        — bulk load into jobs_staging (truncated
                                      per run_id, never touches jobs table)
  Stage C  run_dq_checks()          — null/outlier/duplicate checks on staging;
                                      stamps each row's dq_status; logs issues
                                      to data_quality_log
  Stage D  merge_staging_to_prod()  — single-transaction UPSERT from staging
                                      → jobs; returns (inserted, updated)
  Stage E  load_skills()            — delete-for-job_id + insert (already
                                      incremental; kept identical behaviour)
  Stage F  log_pipeline_run()       — comprehensive pipeline_runs entry

Power BI compatibility:
  ✅  No existing tables, columns, or views are renamed/dropped/modified.
  ✅  New columns (run_id, ingestion_timestamp, etc.) are additive.
  ✅  All 14 KPI views continue to work without change.
─────────────────────────────────────────────────────────────────────────────
"""

import json
import logging
import os
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from colorama import Fore, Style, init as colorama_init
from sqlalchemy import text
from dotenv import load_dotenv

from etl.db import get_engine

colorama_init(autoreset=True)
load_dotenv()

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Columns that must be present in the CSV
REQUIRED_COLUMNS = [
    "job_id", "source", "title", "company", "emirate",
    "salary_aed_monthly", "salary_source", "posted_at",
]

# Columns written to jobs_staging (ordered for SQL clarity)
STAGING_COLS = [
    "job_id", "source", "title", "title_normalized", "company", "city",
    "emirate", "sector", "seniority_band", "employment_type", "remote",
    "salary_min", "salary_max", "salary_currency",
    "salary_aed_monthly", "salary_source", "salary_imputed_flag", "confidence_score",
    "company_size_band", "is_mnc", "company_hq_country", "company_industry",
    "glassdoor_rating", "enrichment_source",
    "skills_count", "completeness_score", "days_since_posted",
    "posted_at", "apply_link",
    "run_id", "ingestion_timestamp",
]

# Columns in the final jobs table (must match schema_updates.sql + original schema)
JOBS_COLS = STAGING_COLS  # same set

# These fields are updated on every re-run (salary may improve each run)
UPSERT_UPDATE_COLS = [
    "salary_aed_monthly", "salary_source", "salary_imputed_flag", "confidence_score",
    "completeness_score", "skills_count", "days_since_posted",
    "company_size_band", "is_mnc", "company_hq_country", "company_industry",
    "glassdoor_rating", "enrichment_source",
    "last_seen_at", "run_id", "ingestion_timestamp",
]

# DQ thresholds
SALARY_MIN_AED   = 1_000
SALARY_MAX_AED   = 250_000
OUTLIER_LOW_AED  = 2_000
OUTLIER_HIGH_AED = 200_000


# ─────────────────────────────────────────────────────────────────────────────
# DDL — Ensure tables exist (idempotent for both fresh and existing DBs)
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_DDL = """
-- ── jobs: create if new, migrate if existing ─────────────────────────────────
CREATE TABLE IF NOT EXISTS jobs (
    job_id               TEXT PRIMARY KEY,
    source               TEXT,
    title                TEXT,
    title_normalized     TEXT,
    company              TEXT,
    city                 TEXT,
    emirate              TEXT,
    sector               TEXT,
    seniority_band       TEXT,
    employment_type      TEXT,
    remote               BOOLEAN,
    salary_min           NUMERIC,
    salary_max           NUMERIC,
    salary_currency      TEXT,
    salary_aed_monthly   NUMERIC,
    salary_source        TEXT,
    company_size_band    TEXT,
    is_mnc               BOOLEAN,
    company_hq_country   TEXT,
    company_industry     TEXT,
    glassdoor_rating     NUMERIC,
    enrichment_source    TEXT,
    skills_count         INTEGER,
    completeness_score   INTEGER,
    days_since_posted    INTEGER,
    posted_at            TIMESTAMP WITH TIME ZONE,
    apply_link           TEXT,
    fetched_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Migration guards: safely add new columns to existing jobs table.
-- Each DO block is independent so one failure doesn't block the others.
DO $$ BEGIN
    ALTER TABLE jobs ADD COLUMN salary_imputed_flag BOOLEAN DEFAULT FALSE;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE jobs ADD COLUMN confidence_score NUMERIC;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE jobs ADD COLUMN run_id UUID;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE jobs ADD COLUMN ingestion_timestamp TIMESTAMP WITH TIME ZONE;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE jobs ADD COLUMN last_seen_at TIMESTAMP WITH TIME ZONE;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS job_skills (
    id                   SERIAL PRIMARY KEY,
    job_id               TEXT,
    title                TEXT,
    title_normalized     TEXT,
    company              TEXT,
    emirate              TEXT,
    sector               TEXT,
    seniority_band       TEXT,
    skill                TEXT,
    salary_aed_monthly   NUMERIC,
    salary_source        TEXT,
    posted_at            TIMESTAMP WITH TIME ZONE
);

-- ── pipeline_runs: create if new, migrate if existing ────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id               SERIAL PRIMARY KEY,
    run_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    db_target        TEXT,
    jobs_total       INTEGER,
    jobs_inserted    INTEGER DEFAULT 0,
    skill_tags       INTEGER,
    salary_coverage  NUMERIC,
    avg_completeness NUMERIC,
    status           TEXT,
    notes            TEXT
);

DO $$ BEGIN
    ALTER TABLE pipeline_runs ADD COLUMN run_id UUID;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE pipeline_runs ADD COLUMN start_time TIMESTAMP WITH TIME ZONE;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE pipeline_runs ADD COLUMN end_time TIMESTAMP WITH TIME ZONE;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE pipeline_runs ADD COLUMN rows_inserted INTEGER DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE pipeline_runs ADD COLUMN rows_updated INTEGER DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE pipeline_runs ADD COLUMN error_message TEXT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS jobs_staging (
    job_id               TEXT,
    source               TEXT,
    title                TEXT,
    title_normalized     TEXT,
    company              TEXT,
    city                 TEXT,
    emirate              TEXT,
    sector               TEXT,
    seniority_band       TEXT,
    employment_type      TEXT,
    remote               BOOLEAN,
    salary_min           NUMERIC,
    salary_max           NUMERIC,
    salary_currency      TEXT,
    salary_aed_monthly   NUMERIC,
    salary_source        TEXT,
    salary_imputed_flag  BOOLEAN,
    confidence_score     NUMERIC,
    company_size_band    TEXT,
    is_mnc               BOOLEAN,
    company_hq_country   TEXT,
    company_industry     TEXT,
    glassdoor_rating     NUMERIC,
    enrichment_source    TEXT,
    skills_count         INTEGER,
    completeness_score   INTEGER,
    days_since_posted    INTEGER,
    posted_at            TIMESTAMP WITH TIME ZONE,
    apply_link           TEXT,
    run_id               UUID,
    ingestion_timestamp  TIMESTAMP WITH TIME ZONE,
    staging_loaded_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    dq_status            TEXT DEFAULT 'pending',
    dq_notes             TEXT
);

CREATE TABLE IF NOT EXISTS data_quality_log (
    id           SERIAL PRIMARY KEY,
    run_id       UUID         NOT NULL,
    job_id       TEXT,
    check_name   TEXT         NOT NULL,
    severity     TEXT         NOT NULL,
    field_name   TEXT,
    message      TEXT,
    logged_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Core indexes
CREATE INDEX IF NOT EXISTS idx_jobs_emirate          ON jobs(emirate);
CREATE INDEX IF NOT EXISTS idx_jobs_sector           ON jobs(sector);
CREATE INDEX IF NOT EXISTS idx_jobs_seniority        ON jobs(seniority_band);
CREATE INDEX IF NOT EXISTS idx_jobs_posted           ON jobs(posted_at);
CREATE INDEX IF NOT EXISTS idx_jobs_salary           ON jobs(salary_aed_monthly);
CREATE INDEX IF NOT EXISTS idx_jobs_completeness     ON jobs(completeness_score);
CREATE INDEX IF NOT EXISTS idx_jobs_source           ON jobs(salary_source);
CREATE INDEX IF NOT EXISTS idx_jobs_run_id           ON jobs(run_id);
CREATE INDEX IF NOT EXISTS idx_jobs_last_seen_at     ON jobs(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_skills_skill          ON job_skills(skill);
CREATE INDEX IF NOT EXISTS idx_skills_emirate        ON job_skills(emirate);
CREATE INDEX IF NOT EXISTS idx_skills_sector         ON job_skills(sector);
CREATE INDEX IF NOT EXISTS idx_skills_job            ON job_skills(job_id);
CREATE INDEX IF NOT EXISTS idx_staging_job_id        ON jobs_staging(job_id);
CREATE INDEX IF NOT EXISTS idx_staging_run_id        ON jobs_staging(run_id);
CREATE INDEX IF NOT EXISTS idx_staging_dq            ON jobs_staging(dq_status);
CREATE INDEX IF NOT EXISTS idx_dq_log_run_id         ON data_quality_log(run_id);
"""


# ─────────────────────────────────────────────────────────────────────────────
# KPI Views  (Power BI — unchanged output schema)
# ─────────────────────────────────────────────────────────────────────────────

VIEWS_DDL = {

    "vw_top_skills": """
        CREATE OR REPLACE VIEW vw_top_skills AS
        WITH top_emirate AS (
            SELECT DISTINCT ON (skill)
                skill,
                emirate AS top_emirate
            FROM job_skills
            WHERE emirate IS NOT NULL
            GROUP BY skill, emirate
            ORDER BY skill, COUNT(*) DESC
        )
        SELECT
            js.skill,
            COUNT(*)                                                         AS demand_count,
            COUNT(DISTINCT js.emirate)                                       AS emirate_spread,
            COUNT(DISTINCT js.sector)                                        AS sector_spread,
            COUNT(DISTINCT js.seniority_band)                                AS seniority_spread,
            ROUND(AVG(js.salary_aed_monthly) FILTER (WHERE js.salary_aed_monthly > 0)) AS avg_salary_aed,
            COUNT(*) FILTER (WHERE js.posted_at >= NOW() - INTERVAL '30 days')
                                                                             AS demand_last_30d,
            COUNT(*) FILTER (WHERE js.posted_at BETWEEN NOW() - INTERVAL '60 days'
                                              AND NOW() - INTERVAL '30 days')
                                                                             AS demand_prior_30d,
            CASE
                WHEN COUNT(*) FILTER (WHERE js.posted_at >= NOW() - INTERVAL '30 days') >
                     COUNT(*) FILTER (WHERE js.posted_at BETWEEN NOW() - INTERVAL '60 days'
                                                     AND NOW() - INTERVAL '30 days')
                    THEN 'Rising'
                WHEN COUNT(*) FILTER (WHERE js.posted_at >= NOW() - INTERVAL '30 days') <
                     COUNT(*) FILTER (WHERE js.posted_at BETWEEN NOW() - INTERVAL '60 days'
                                                     AND NOW() - INTERVAL '30 days')
                    THEN 'Falling'
                ELSE 'Stable'
            END                                                              AS trend_direction,
            te.top_emirate                                                   AS emirate
        FROM job_skills js
        LEFT JOIN top_emirate te USING (skill)
        WHERE js.skill IS NOT NULL AND js.skill != ''
        GROUP BY js.skill, te.top_emirate
        ORDER BY demand_count DESC
        LIMIT 50;
    """,

    "vw_hiring_velocity": """
        CREATE OR REPLACE VIEW vw_hiring_velocity AS
        SELECT
            DATE_TRUNC('week', posted_at)  AS week_start,
            emirate,
            sector,
            seniority_band,
            COUNT(*)                       AS jobs_posted,
            ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed,
            ROUND(AVG(COUNT(*)) OVER (
                PARTITION BY emirate, sector
                ORDER BY DATE_TRUNC('week', posted_at)
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            ), 0)                          AS rolling_4w_avg
        FROM jobs
        WHERE posted_at IS NOT NULL
        GROUP BY 1, 2, 3, 4
        ORDER BY 1 DESC;
    """,

    "vw_remote_analysis": """
        CREATE OR REPLACE VIEW vw_remote_analysis AS
        SELECT
            sector, emirate, seniority_band, remote,
            COUNT(*)                                    AS job_count,
            ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY sector, emirate), 1)
                                                        AS pct_of_sector_emirate
        FROM jobs
        GROUP BY sector, emirate, seniority_band, remote;
    """,

    "vw_salary_bands": """
        CREATE OR REPLACE VIEW vw_salary_bands AS
        SELECT
            title_normalized, emirate, sector, seniority_band, salary_source,
            ROUND(AVG(salary_aed_monthly))                                          AS avg_salary_aed,
            ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS p25_aed,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS median_aed,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS p75_aed,
            COUNT(*)                                                                AS sample_size,
            ROUND(100.0 * COUNT(*) FILTER (WHERE salary_source = 'reported') / COUNT(*), 0)
                                                                                    AS pct_reported
        FROM jobs
        WHERE salary_aed_monthly BETWEEN 2000 AND 200000
        GROUP BY title_normalized, emirate, sector, seniority_band, salary_source
        HAVING COUNT(*) >= 2
        ORDER BY avg_salary_aed DESC;
    """,

    "vw_skill_salary_premium": """
        CREATE OR REPLACE VIEW vw_skill_salary_premium AS
        WITH baseline AS (
            SELECT AVG(salary_aed_monthly) AS overall_avg
            FROM jobs WHERE salary_aed_monthly BETWEEN 2000 AND 200000
        ),
        skill_salaries AS (
            SELECT js.skill, js.sector, js.emirate,
                   ROUND(AVG(j.salary_aed_monthly)) AS avg_salary_aed,
                   COUNT(*) AS job_count
            FROM job_skills js
            JOIN jobs j ON js.job_id = j.job_id
            WHERE j.salary_aed_monthly BETWEEN 2000 AND 200000
            GROUP BY js.skill, js.sector, js.emirate
            HAVING COUNT(*) >= 3
        )
        SELECT ss.skill, ss.sector, ss.emirate,
               ss.avg_salary_aed, ss.job_count,
               ROUND((ss.avg_salary_aed - b.overall_avg) / b.overall_avg * 100, 1) AS salary_premium_pct
        FROM skill_salaries ss
        CROSS JOIN baseline b
        ORDER BY salary_premium_pct DESC;
    """,

    "vw_skill_heatmap": """
        CREATE OR REPLACE VIEW vw_skill_heatmap AS
        SELECT
            job_id, skill, emirate, sector, seniority_band,
            COUNT(*) AS count,
            ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed
        FROM job_skills
        WHERE skill IS NOT NULL AND skill != ''
        GROUP BY job_id, skill, emirate, sector, seniority_band;
    """,

    "vw_skill_cooccurrence": """
        CREATE OR REPLACE VIEW vw_skill_cooccurrence AS
        SELECT a.skill AS skill_a, b.skill AS skill_b,
               COUNT(*) AS cooccurrence_count,
               COUNT(DISTINCT a.emirate) AS emirate_spread
        FROM job_skills a
        JOIN job_skills b ON a.job_id = b.job_id AND a.skill < b.skill
        WHERE a.skill IS NOT NULL AND b.skill IS NOT NULL
        GROUP BY a.skill, b.skill
        HAVING COUNT(*) >= 5
        ORDER BY cooccurrence_count DESC
        LIMIT 200;
    """,

    "vw_emerging_skills": """
        CREATE OR REPLACE VIEW vw_emerging_skills AS
        WITH current_period AS (
            SELECT skill, COUNT(*) AS current_count FROM job_skills
            WHERE posted_at >= NOW() - INTERVAL '30 days' GROUP BY skill
        ),
        prior_period AS (
            SELECT skill, COUNT(*) AS prior_count FROM job_skills
            WHERE posted_at BETWEEN NOW() - INTERVAL '60 days' AND NOW() - INTERVAL '30 days'
            GROUP BY skill
        )
        SELECT c.skill, c.current_count,
               COALESCE(p.prior_count, 0) AS prior_count,
               c.current_count - COALESCE(p.prior_count, 0) AS absolute_growth,
               CASE WHEN COALESCE(p.prior_count, 0) = 0 THEN NULL
                    ELSE ROUND(100.0 * (c.current_count - p.prior_count) / p.prior_count, 1)
               END AS growth_pct
        FROM current_period c
        LEFT JOIN prior_period p USING (skill)
        WHERE c.current_count >= 5
        ORDER BY absolute_growth DESC;
    """,

    "vw_company_radar": """
        CREATE OR REPLACE VIEW vw_company_radar AS
        SELECT
            j.company, j.emirate, j.sector, j.company_size_band, j.is_mnc, j.company_hq_country,
            COUNT(*) AS open_roles,
            ROUND(AVG(j.salary_aed_monthly) FILTER (WHERE j.salary_aed_monthly > 0)) AS avg_salary_aed,
            COUNT(DISTINCT DATE_TRUNC('week', j.posted_at)) AS active_weeks,
            ROUND(AVG(j.glassdoor_rating) FILTER (WHERE j.glassdoor_rating IS NOT NULL), 1) AS avg_glassdoor_rating,
            ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT DATE_TRUNC('week', j.posted_at)), 0), 1) AS hiring_intensity
        FROM jobs j
        WHERE j.company IS NOT NULL AND j.company != ''
        GROUP BY j.company, j.emirate, j.sector, j.company_size_band, j.is_mnc, j.company_hq_country
        ORDER BY open_roles DESC
        LIMIT 100;
    """,

    "vw_mnc_vs_local": """
        CREATE OR REPLACE VIEW vw_mnc_vs_local AS
        SELECT
            CASE WHEN is_mnc THEN 'MNC' ELSE 'Local / Regional' END AS company_type,
            sector, emirate,
            COUNT(*) AS job_count,
            ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed,
            ROUND(AVG(skills_count), 1) AS avg_skills_required
        FROM jobs WHERE is_mnc IS NOT NULL
        GROUP BY 1, 2, 3;
    """,

    "vw_pipeline_health": """
        CREATE OR REPLACE VIEW vw_pipeline_health AS
        SELECT
            COUNT(*) AS total_jobs,
            COUNT(*) FILTER (WHERE source = 'jsearch')  AS jsearch_jobs,
            COUNT(*) FILTER (WHERE source = 'adzuna')   AS adzuna_jobs,
            ROUND(100.0 * COUNT(*) FILTER (WHERE salary_aed_monthly > 0) / NULLIF(COUNT(*), 0), 1) AS pct_salary_filled,
            ROUND(100.0 * COUNT(*) FILTER (WHERE salary_source = 'reported') / NULLIF(COUNT(*), 0), 1) AS pct_salary_reported,
            ROUND(100.0 * COUNT(*) FILTER (WHERE salary_source LIKE 'imputed%') / NULLIF(COUNT(*), 0), 1) AS pct_salary_imputed,
            ROUND(AVG(completeness_score), 1) AS avg_completeness_score,
            ROUND(AVG(skills_count), 1) AS avg_skills_per_job,
            MAX(posted_at) AS newest_posting,
            MIN(posted_at) AS oldest_posting,
            ROUND(AVG(CURRENT_DATE - posted_at::date), 0) AS avg_days_since_posted,
            COUNT(DISTINCT company) AS unique_companies,
            COUNT(DISTINCT emirate) AS unique_emirates
        FROM jobs;
    """,

    "vw_field_coverage": """
        CREATE OR REPLACE VIEW vw_field_coverage AS
        SELECT 'title'              AS field, ROUND(100.0 * COUNT(*) FILTER (WHERE title IS NOT NULL AND title != '') / COUNT(*), 1) AS pct_filled FROM jobs UNION ALL
        SELECT 'company',                     ROUND(100.0 * COUNT(*) FILTER (WHERE company IS NOT NULL AND company != '') / COUNT(*), 1) FROM jobs UNION ALL
        SELECT 'emirate',                     ROUND(100.0 * COUNT(*) FILTER (WHERE emirate IS NOT NULL) / COUNT(*), 1) FROM jobs UNION ALL
        SELECT 'sector',                      ROUND(100.0 * COUNT(*) FILTER (WHERE sector != 'Other') / COUNT(*), 1) FROM jobs UNION ALL
        SELECT 'salary_aed_monthly',          ROUND(100.0 * COUNT(*) FILTER (WHERE salary_aed_monthly > 0) / COUNT(*), 1) FROM jobs UNION ALL
        SELECT 'salary_reported',             ROUND(100.0 * COUNT(*) FILTER (WHERE salary_source = 'reported') / COUNT(*), 1) FROM jobs UNION ALL
        SELECT 'seniority_band',              ROUND(100.0 * COUNT(*) FILTER (WHERE seniority_band != 'Mid-Level') / COUNT(*), 1) FROM jobs UNION ALL
        SELECT 'company_size_band',           ROUND(100.0 * COUNT(*) FILTER (WHERE company_size_band IS NOT NULL AND company_size_band != 'Unknown') / COUNT(*), 1) FROM jobs UNION ALL
        SELECT 'posted_at',                   ROUND(100.0 * COUNT(*) FILTER (WHERE posted_at IS NOT NULL) / COUNT(*), 1) FROM jobs UNION ALL
        SELECT 'skills_count >= 1',           ROUND(100.0 * COUNT(*) FILTER (WHERE skills_count >= 1) / COUNT(*), 1) FROM jobs;
    """,

    "vw_pipeline_runs": """
        CREATE OR REPLACE VIEW vw_pipeline_runs AS
        SELECT
            id,
            COALESCE(start_time, run_at)           AS run_at,
            db_target,
            jobs_total,
            COALESCE(rows_inserted, jobs_inserted) AS jobs_inserted,
            skill_tags,
            ROUND(salary_coverage, 1)              AS salary_coverage_pct,
            ROUND(avg_completeness, 1)             AS avg_completeness_score,
            status,
            notes,
            run_id,
            start_time,
            end_time,
            rows_inserted,
            rows_updated,
            error_message
        FROM pipeline_runs
        ORDER BY COALESCE(start_time, run_at) DESC;
    """,

    "vw_jobs_detail": """
        CREATE OR REPLACE VIEW vw_jobs_detail AS
        SELECT
            job_id,
            source,
            title,
            title_normalized,
            company,
            city,
            emirate,
            sector,
            seniority_band,
            employment_type,
            remote,
            salary_aed_monthly,
            salary_source,
            company_size_band,
            is_mnc,
            company_hq_country,
            skills_count,
            completeness_score,
            posted_at,
            (CURRENT_DATE - posted_at::date) AS days_since_posted,
            apply_link,
            salary_imputed_flag,
            confidence_score,
            run_id,
            ingestion_timestamp,
            last_seen_at
        FROM jobs
        ORDER BY posted_at DESC NULLS LAST;
    """,
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _log(msg: str, level: str = "info") -> None:
    colour = {
        "info":    Fore.CYAN,
        "ok":      Fore.GREEN,
        "warn":    Fore.YELLOW,
        "error":   Fore.RED,
    }.get(level, "")
    prefix = {"info": "  ·", "ok": "  ✓", "warn": "  ⚠", "error": "  ✗"}.get(level, "  ·")
    print(f"{colour}{prefix} {msg}{Style.RESET_ALL}")
    getattr(logger, level if level != "ok" else "info")(msg)


def _coerce_row(row_dict: dict) -> dict:
    """Sanitise a row dict: NaN → None, booleans cast, large ints cast."""
    out = {}
    for k, v in row_dict.items():
        if isinstance(v, float) and np.isnan(v):
            out[k] = None
        elif k in ("remote", "is_mnc", "salary_imputed_flag") and v is not None:
            out[k] = bool(v)
        else:
            out[k] = v
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Stage A — Schema Validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """
    Validates required columns exist and coerces basic types.
    Returns (cleaned_df, list_of_validation_issues).
    Issues are logged to data_quality_log by the caller.
    """
    issues: list[dict] = []

    # 1. Required columns
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Schema validation failed — missing required columns: {missing_cols}\n"
            "Re-run transform.py to regenerate jobs_clean.csv."
        )

    df = df.copy()

    # 2. Coerce data types
    df["salary_aed_monthly"] = pd.to_numeric(df["salary_aed_monthly"], errors="coerce").fillna(0.0)
    df["skills_count"]       = pd.to_numeric(df["skills_count"],       errors="coerce").fillna(0).astype(int)
    df["completeness_score"] = pd.to_numeric(df["completeness_score"], errors="coerce").fillna(0).astype(int)
    df["posted_at"]          = pd.to_datetime(df["posted_at"],         errors="coerce", utc=True)

    for bool_col in ("remote", "is_mnc", "salary_imputed_flag"):
        if bool_col in df.columns:
            df[bool_col] = df[bool_col].fillna(False).astype(bool)

    # 3. Drop rows with no job_id (cannot upsert without PK)
    null_id_mask = df["job_id"].isna() | (df["job_id"].astype(str).str.strip() == "")
    if null_id_mask.any():
        n = int(null_id_mask.sum())
        issues.append({
            "check_name": "null_job_id", "severity": "error",
            "field_name": "job_id",
            "message": f"{n} rows have null/empty job_id — dropped before load",
        })
        df = df[~null_id_mask]

    # 4. Deduplicate on job_id (keep last, which has the richest salary)
    before = len(df)
    df = df.drop_duplicates(subset=["job_id"], keep="last")
    dupes = before - len(df)
    if dupes:
        issues.append({
            "check_name": "duplicate_job_id", "severity": "warning",
            "field_name": "job_id",
            "message": f"{dupes} duplicate job_ids deduplicated (kept last)",
        })

    _log(f"Schema validation passed  ({len(df):,} rows, {len(issues)} issues)", "ok")
    return df, issues


# ─────────────────────────────────────────────────────────────────────────────
# Stage B — Load to Staging
# ─────────────────────────────────────────────────────────────────────────────

def load_to_staging(engine, df: pd.DataFrame, run_id: str) -> None:
    """
    Clears staging rows for this run_id, then bulk-inserts incoming data.
    Never touches the production jobs table.
    """
    # Ensure only valid staging columns are written
    cols_available = [c for c in STAGING_COLS if c in df.columns]
    df_stage = df[cols_available].copy()

    # Coerce to SQL-safe types
    for bool_col in ("remote", "is_mnc", "salary_imputed_flag"):
        if bool_col in df_stage.columns:
            df_stage[bool_col] = df_stage[bool_col].fillna(False).astype(bool)

    df_stage["run_id"] = run_id
    df_stage["dq_status"] = "pending"

    with engine.begin() as conn:
        # Remove any previous attempt for this run_id (idempotent re-runs)
        conn.execute(text("DELETE FROM jobs_staging WHERE run_id = :rid"), {"rid": run_id})
        df_stage.to_sql("jobs_staging", conn, if_exists="append", index=False,
                        method="multi", chunksize=500)

    _log(f"Staging loaded: {len(df_stage):,} rows (run_id={run_id[:8]}…)", "ok")


# ─────────────────────────────────────────────────────────────────────────────
# Stage C — Data Quality Checks
# ─────────────────────────────────────────────────────────────────────────────

def run_dq_checks(engine, run_id: str, validation_issues: list[dict]) -> dict:
    """
    Runs DQ checks on jobs_staging for this run_id.
    Updates dq_status per row and logs all issues to data_quality_log.
    Returns summary counts.
    """
    dq_issues: list[dict] = list(validation_issues)  # carry forward schema issues

    with engine.begin() as conn:

        # ── A. Null checks on critical fields ────────────────────────────────
        critical_nulls = {
            "title":   "title IS NULL OR title = ''",
            "company": "company IS NULL OR company = ''",
            "emirate": "emirate IS NULL",
        }
        for field, condition in critical_nulls.items():
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM jobs_staging WHERE run_id = :rid AND ({condition})"),
                {"rid": run_id},
            ).scalar()
            if result:
                dq_issues.append({
                    "check_name": f"null_{field}", "severity": "warning",
                    "field_name": field,
                    "message": f"{result:,} rows have null/empty {field}",
                })

        # ── B. Salary outlier check ──────────────────────────────────────────
        outlier_result = conn.execute(
            text("""
                SELECT COUNT(*) FROM jobs_staging
                WHERE run_id = :rid
                  AND salary_aed_monthly > 0
                  AND (salary_aed_monthly < :lo OR salary_aed_monthly > :hi)
            """),
            {"rid": run_id, "lo": OUTLIER_LOW_AED, "hi": OUTLIER_HIGH_AED},
        ).scalar()
        if outlier_result:
            dq_issues.append({
                "check_name": "salary_outlier", "severity": "warning",
                "field_name": "salary_aed_monthly",
                "message": f"{outlier_result:,} rows have salary outside AED {OUTLIER_LOW_AED:,}–{OUTLIER_HIGH_AED:,}",
            })

        # ── C. Duplicate detection (same job_id submitted twice this run) ───
        dup_result = conn.execute(
            text("""
                SELECT COUNT(*) FROM (
                    SELECT job_id, COUNT(*) AS n
                    FROM jobs_staging
                    WHERE run_id = :rid
                    GROUP BY job_id HAVING COUNT(*) > 1
                ) t
            """),
            {"rid": run_id},
        ).scalar()
        if dup_result:
            dq_issues.append({
                "check_name": "staging_duplicates", "severity": "warning",
                "field_name": "job_id",
                "message": f"{dup_result:,} job_ids appear more than once in staging",
            })

        # ── D. Cross-source content-hash duplicate check ─────────────────────
        # Catches jobs where job_id differs across sources but content is identical
        # (same normalised title + company + emirate). These should have been
        # removed by dedup_cross_source() in transform.py — if any survive here
        # it means the pipeline was run without the transform fix applied.
        content_dup_result = conn.execute(
            text("""
                SELECT COUNT(*) FROM (
                    SELECT
                        MD5(
                            LOWER(TRIM(COALESCE(title_normalized, ''))) || '|' ||
                            LOWER(TRIM(COALESCE(company, '')))          || '|' ||
                            LOWER(TRIM(COALESCE(emirate, '')))
                        ) AS content_hash,
                        COUNT(*) AS n
                    FROM jobs_staging
                    WHERE run_id = :rid
                    GROUP BY content_hash
                    HAVING COUNT(*) > 1
                ) t
            """),
            {"rid": run_id},
        ).scalar()
        if content_dup_result:
            dq_issues.append({
                "check_name": "cross_source_duplicates", "severity": "warning",
                "field_name": "title_normalized,company,emirate",
                "message": (
                    f"{content_dup_result:,} content-hash groups have >1 row "
                    f"(same title+company+emirate, different job_id). "
                    f"Run transform.py to deduplicate before next load."
                ),
            })

        # ── D. Stamp dq_status on each row ──────────────────────────────────
        # For now, mark all as 'valid' — individual-row errors are handled
        # upstream in validate_dataframe(). Outlier rows get 'warning'.
        conn.execute(
            text("UPDATE jobs_staging SET dq_status = 'valid' WHERE run_id = :rid"),
            {"rid": run_id},
        )
        conn.execute(
            text("""
                UPDATE jobs_staging
                SET dq_status = 'warning',
                    dq_notes  = 'salary_outlier'
                WHERE run_id = :rid
                  AND salary_aed_monthly > 0
                  AND (salary_aed_monthly < :lo OR salary_aed_monthly > :hi)
            """),
            {"rid": run_id, "lo": OUTLIER_LOW_AED, "hi": OUTLIER_HIGH_AED},
        )

        # ── E. Log all issues to data_quality_log ───────────────────────────
        if dq_issues:
            for issue in dq_issues:
                conn.execute(
                    text("""
                        INSERT INTO data_quality_log (run_id, check_name, severity, field_name, message)
                        VALUES (:run_id, :check_name, :severity, :field_name, :message)
                    """),
                    {
                        "run_id":     run_id,
                        "check_name": issue["check_name"],
                        "severity":   issue["severity"],
                        "field_name": issue.get("field_name"),
                        "message":    issue["message"],
                    },
                )

        # ── F. Summary ───────────────────────────────────────────────────────
        counts = conn.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (WHERE dq_status = 'valid')   AS valid,
                    COUNT(*) FILTER (WHERE dq_status = 'warning') AS warning,
                    COUNT(*) FILTER (WHERE dq_status = 'invalid') AS invalid
                FROM jobs_staging WHERE run_id = :rid
            """),
            {"rid": run_id},
        ).mappings().one()

    summary = dict(counts)
    errors   = sum(1 for i in dq_issues if i["severity"] == "error")
    warnings = sum(1 for i in dq_issues if i["severity"] == "warning")

    _log(
        f"DQ checks: {summary['valid']:,} valid  "
        f"{summary['warning']:,} warnings  "
        f"{summary['invalid']:,} invalid  "
        f"| issues logged: {errors} errors, {warnings} warnings",
        "ok" if errors == 0 else "warn",
    )
    for issue in dq_issues:
        _log(f"  [{issue['severity'].upper()}] {issue['check_name']}: {issue['message']}", "warn")

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Stage D — Merge Staging → Production (transactional UPSERT)
# ─────────────────────────────────────────────────────────────────────────────

def merge_staging_to_production(engine, run_id: str) -> tuple[int, int]:
    """
    Single-transaction UPSERT from jobs_staging → jobs.
    Uses PostgreSQL's xmax trick to count inserted vs updated rows.
    Only moves rows with dq_status IN ('valid', 'warning').

    Returns (rows_inserted, rows_updated).

    If ANY step fails → the entire transaction is rolled back automatically
    by SQLAlchemy's engine.begin() context manager.
    """
    # Build SET clause for DO UPDATE — only updateable fields
    set_clause = ",\n                    ".join(
        f"{col} = EXCLUDED.{col}" for col in UPSERT_UPDATE_COLS
    )

    # Columns to select from staging (all STAGING_COLS that exist in jobs)
    insert_cols = ", ".join(STAGING_COLS)
    select_cols = ", ".join(STAGING_COLS)

    upsert_sql = text(f"""
        WITH upsert AS (
            INSERT INTO jobs (
                {insert_cols},
                last_seen_at
            )
            SELECT
                {select_cols},
                NOW() AS last_seen_at
            FROM jobs_staging
            WHERE run_id = :run_id
              AND dq_status IN ('valid', 'warning')
            ON CONFLICT (job_id) DO UPDATE
            SET
                    {set_clause}
            RETURNING job_id, (xmax = 0) AS is_new
        )
        SELECT
            COUNT(*) FILTER (WHERE is_new)       AS inserted,
            COUNT(*) FILTER (WHERE NOT is_new)   AS updated
        FROM upsert
    """)

    with engine.begin() as conn:
        result = conn.execute(upsert_sql, {"run_id": run_id}).mappings().one()

    inserted = int(result["inserted"])
    updated  = int(result["updated"])

    _log(
        f"Merge complete: {inserted:,} inserted  {updated:,} updated  "
        f"({inserted + updated:,} total)",
        "ok",
    )
    return inserted, updated


# ─────────────────────────────────────────────────────────────────────────────
# Stage E — Skills (incremental delete-for-job + insert)
# ─────────────────────────────────────────────────────────────────────────────

def load_skills(engine, df: pd.DataFrame) -> int:
    """
    Replaces skill tags for affected job_ids.
    Uses DELETE WHERE job_id = ANY(ids) + INSERT — no full-table delete.
    This is already incremental: only jobs present in this run are refreshed.
    """
    if df.empty:
        return 0

    skill_cols = [c for c in df.columns if c in [
        "job_id", "title", "title_normalized", "company", "emirate",
        "sector", "seniority_band", "skill", "salary_aed_monthly",
        "salary_source", "posted_at",
    ]]
    df_out  = df[skill_cols].copy()
    job_ids = df_out["job_id"].dropna().unique().tolist()

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM job_skills WHERE job_id = ANY(:ids)"),
            {"ids": job_ids},
        )
        df_out.to_sql("job_skills", conn, if_exists="append", index=False,
                      method="multi", chunksize=1000)

    _log(f"Skills: {len(df_out):,} tag rows for {len(job_ids):,} jobs", "ok")
    return len(df_out)


# ─────────────────────────────────────────────────────────────────────────────
# Stage F — Pipeline Run Logging
# ─────────────────────────────────────────────────────────────────────────────

def start_pipeline_run(engine, run_id: str, start_time: datetime) -> int:
    """
    Inserts a 'running' row into pipeline_runs at pipeline start.
    Returns the row id for later update.
    """
    with engine.begin() as conn:
        row_id = conn.execute(
            text("""
                INSERT INTO pipeline_runs
                    (run_id, start_time, db_target, status)
                VALUES
                    (:run_id, :start_time, :db_target, 'running')
                RETURNING id
            """),
            {
                "run_id":     run_id,
                "start_time": start_time,
                "db_target":  os.getenv("DB_TARGET", "local"),
            },
        ).scalar()
    _log(f"Pipeline run started (id={row_id}, run_id={run_id[:8]}…)", "info")
    return row_id


def finish_pipeline_run(
    engine,
    row_id: int,
    run_id: str,
    meta: dict,
    rows_inserted: int,
    rows_updated: int,
    n_skills: int,
    status: str = "success",
    error_message: Optional[str] = None,
) -> None:
    """Updates the pipeline_runs row written by start_pipeline_run."""
    end_time = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE pipeline_runs SET
                    end_time        = :end_time,
                    status          = :status,
                    jobs_total      = :jobs_total,
                    jobs_inserted   = :rows_inserted,
                    rows_inserted   = :rows_inserted,
                    rows_updated    = :rows_updated,
                    skill_tags      = :skill_tags,
                    salary_coverage = :salary_coverage,
                    avg_completeness = :avg_completeness,
                    error_message   = :error_message,
                    notes           = :notes
                WHERE id = :row_id
            """),
            {
                "row_id":          row_id,
                "end_time":        end_time,
                "status":          status,
                "jobs_total":      meta.get("total_jobs", 0),
                "rows_inserted":   rows_inserted,
                "rows_updated":    rows_updated,
                "skill_tags":      n_skills,
                "salary_coverage": meta.get("salary_coverage_pct", 0),
                "avg_completeness": meta.get("avg_completeness", 0),
                "error_message":   error_message,
                "notes":           json.dumps({
                    "salary_source_split": meta.get("salary_source_split", {}),
                    "emirate_breakdown":   meta.get("emirate_breakdown", {}),
                    "db_target":           os.getenv("DB_TARGET", "local"),
                }),
            },
        )
    _log(
        f"Run {row_id} finished — status={status}  "
        f"inserted={rows_inserted:,}  updated={rows_updated:,}",
        "ok" if status in ("success", "sync") else "error",
    )


def create_views(engine) -> None:
    """
    Creates/replaces each KPI view in its own independent transaction.
    A failure on one view (e.g. column-order conflict) is logged as a warning
    but does NOT abort the remaining views.
    """
    for name, ddl in VIEWS_DDL.items():
        try:
            with engine.begin() as conn:
                conn.execute(text(ddl))
            _log(f"{name}", "ok")
        except Exception as exc:
            _log(f"{name}: {exc}", "warn")


# ─────────────────────────────────────────────────────────────────────────────
# Main runner
# ─────────────────────────────────────────────────────────────────────────────

def run_load(run_id: Optional[str] = None, pipeline_start: Optional[datetime] = None) -> None:
    if run_id is None:
        run_id = str(uuid.uuid4())
    if pipeline_start is None:
        pipeline_start = datetime.now(timezone.utc)

    engine   = get_engine()
    row_id   = None
    n_skills = 0
    rows_inserted = rows_updated = 0
    meta: dict = {}

    try:
        # ── Bootstrap tables + views ─────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Creating / verifying tables…{Style.RESET_ALL}")
        with engine.begin() as conn:
            conn.execute(text(SCHEMA_DDL))
        _log("Tables and indexes ready", "ok")

        print(f"\n  {Fore.CYAN}Creating KPI views…{Style.RESET_ALL}")
        create_views(engine)

        # ── Read processed CSVs ───────────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Reading processed CSVs…{Style.RESET_ALL}")
        df_jobs   = pd.read_csv("data/processed/jobs_clean.csv",  low_memory=False)
        df_skills = pd.read_csv("data/processed/skills_long.csv", low_memory=False)

        meta_path = Path("data/processed/transform_meta.json")
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)

        _log(f"Read {len(df_jobs):,} jobs, {len(df_skills):,} skill tags", "ok")

        # ── Start pipeline run record ─────────────────────────────────────────
        row_id = start_pipeline_run(engine, run_id, pipeline_start)

        # ── Stage A: Schema validation ────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Stage A — Schema validation…{Style.RESET_ALL}")
        df_jobs, validation_issues = validate_dataframe(df_jobs)

        # ── Stage B: Load to staging ──────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Stage B — Loading to staging table…{Style.RESET_ALL}")
        load_to_staging(engine, df_jobs, run_id)

        # ── Stage C: DQ checks ────────────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Stage C — Data quality checks…{Style.RESET_ALL}")
        dq_summary = run_dq_checks(engine, run_id, validation_issues)

        # ── Stage D: Merge staging → production (transactional UPSERT) ────────
        print(f"\n  {Fore.CYAN}Stage D — Merging staging → production (UPSERT)…{Style.RESET_ALL}")
        rows_inserted, rows_updated = merge_staging_to_production(engine, run_id)

        # ── Stage E: Skills ───────────────────────────────────────────────────
        print(f"\n  {Fore.CYAN}Stage E — Loading skill tags (incremental)…{Style.RESET_ALL}")
        n_skills = load_skills(engine, df_skills)

        # ── Stage F: Finish pipeline run record ───────────────────────────────
        finish_pipeline_run(
            engine, row_id, run_id, meta,
            rows_inserted, rows_updated, n_skills,
            status="success",
        )

        print(f"\n  {Fore.GREEN}✅  Load complete{Style.RESET_ALL}")
        print(f"     Inserted     : {rows_inserted:,}")
        print(f"     Updated      : {rows_updated:,}")
        print(f"     Skill tags   : {n_skills:,}")
        print(f"     DQ summary   : {dq_summary}")
        print(f"     run_id       : {run_id}")

    except Exception as exc:
        err_msg = traceback.format_exc()
        _log(f"Load FAILED: {exc}", "error")
        if row_id is not None:
            try:
                finish_pipeline_run(
                    engine, row_id, run_id, meta,
                    rows_inserted, rows_updated, n_skills,
                    status="failed",
                    error_message=err_msg[-1000:],   # truncate for DB
                )
            except Exception:
                pass  # don't mask the original error
        raise


if __name__ == "__main__":
    run_load()