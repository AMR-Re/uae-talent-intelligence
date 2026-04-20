"""
etl/load_db.py
─────────────────────────────────────────────────────────────────────────────
Reads clean CSVs from data/processed/ and upserts into the target database.
Target controlled by DB_TARGET in .env ("local" or "supabase").
Idempotent — safe to run multiple times (uses ON CONFLICT DO NOTHING for jobs,
DELETE + INSERT for job_skills to keep skill tags fresh).

Creates:
  Tables:  jobs, job_skills, pipeline_runs
  Views:   14 KPI views used by Power BI (see sql/kpi_views.sql for docs)
─────────────────────────────────────────────────────────────────────────────
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from colorama import Fore, Style, init as colorama_init
from sqlalchemy import text
from dotenv import load_dotenv

from etl.db import get_engine

colorama_init(autoreset=True)
load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# DDL — Tables
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_DDL = """
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

    -- Salary
    salary_min           NUMERIC,
    salary_max           NUMERIC,
    salary_currency      TEXT,
    salary_aed_monthly   NUMERIC,
    salary_source        TEXT,          -- 'reported' | 'imputed_peer_L1..L4' | 'imputed_model' | 'imputed_benchmark'

    -- Company enrichment
    company_size_band    TEXT,
    is_mnc               BOOLEAN,
    company_hq_country   TEXT,
    company_industry     TEXT,
    glassdoor_rating     NUMERIC,
    enrichment_source    TEXT,

    -- Quality
    skills_count         INTEGER,
    completeness_score   INTEGER,
    days_since_posted    INTEGER,

    posted_at            TIMESTAMP WITH TIME ZONE,
    apply_link           TEXT,
    fetched_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

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

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    db_target       TEXT,
    jobs_total      INTEGER,
    jobs_inserted   INTEGER,
    skill_tags      INTEGER,
    salary_coverage NUMERIC,
    avg_completeness NUMERIC,
    status          TEXT,
    notes           TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_jobs_emirate         ON jobs(emirate);
CREATE INDEX IF NOT EXISTS idx_jobs_sector          ON jobs(sector);
CREATE INDEX IF NOT EXISTS idx_jobs_seniority       ON jobs(seniority_band);
CREATE INDEX IF NOT EXISTS idx_jobs_posted          ON jobs(posted_at);
CREATE INDEX IF NOT EXISTS idx_jobs_salary          ON jobs(salary_aed_monthly);
CREATE INDEX IF NOT EXISTS idx_jobs_completeness    ON jobs(completeness_score);
CREATE INDEX IF NOT EXISTS idx_jobs_source          ON jobs(salary_source);
CREATE INDEX IF NOT EXISTS idx_skills_skill         ON job_skills(skill);
CREATE INDEX IF NOT EXISTS idx_skills_emirate       ON job_skills(emirate);
CREATE INDEX IF NOT EXISTS idx_skills_sector        ON job_skills(sector);
CREATE INDEX IF NOT EXISTS idx_skills_seniority     ON job_skills(seniority_band);
CREATE INDEX IF NOT EXISTS idx_skills_job           ON job_skills(job_id);
"""


# ─────────────────────────────────────────────────────────────────────────────
# DDL — Views (Power BI connects to these)
# ─────────────────────────────────────────────────────────────────────────────

VIEWS_DDL = {

    # ── PAGE 1: Market Overview ───────────────────────────────────────────────

    # FIX: Added top_emirate CTE so the tooltip page (Page 7) has a real text
    # emirate value. emirate_spread (COUNT DISTINCT int) is kept alongside it.
    # Also added trend_direction derived column for Power BI conditional
    # formatting — was missing from the original load_db.py version.
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
            COUNT(*)                                                        AS demand_count,
            COUNT(DISTINCT js.emirate)                                      AS emirate_spread,
            COUNT(DISTINCT js.sector)                                       AS sector_spread,
            COUNT(DISTINCT js.seniority_band)                               AS seniority_spread,
            ROUND(AVG(js.salary_aed_monthly) FILTER (WHERE js.salary_aed_monthly > 0)) AS avg_salary_aed,
            COUNT(*) FILTER (WHERE js.posted_at >= NOW() - INTERVAL '30 days')
                                                                            AS demand_last_30d,
            COUNT(*) FILTER (WHERE js.posted_at BETWEEN NOW() - INTERVAL '60 days'
                                              AND     NOW() - INTERVAL '30 days')
                                                                            AS demand_prior_30d,
            CASE
                WHEN COUNT(*) FILTER (WHERE js.posted_at >= NOW() - INTERVAL '30 days') >
                     COUNT(*) FILTER (WHERE js.posted_at BETWEEN NOW() - INTERVAL '60 days'
                                                     AND     NOW() - INTERVAL '30 days')
                THEN 'Rising'
                WHEN COUNT(*) FILTER (WHERE js.posted_at >= NOW() - INTERVAL '30 days') <
                     COUNT(*) FILTER (WHERE js.posted_at BETWEEN NOW() - INTERVAL '60 days'
                                                     AND     NOW() - INTERVAL '30 days')
                THEN 'Falling'
                ELSE 'Stable'
            END                                                             AS trend_direction,
            te.top_emirate                                                  AS emirate
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
            -- FIX: rolling_4w_avg was missing from the original load_db.py version
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
            sector,
            emirate,
            seniority_band,
            remote,
            COUNT(*)                                    AS job_count,
            ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed,
            -- FIX: pct_of_sector_emirate was missing from the original load_db.py version
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY sector, emirate), 1)
                                                        AS pct_of_sector_emirate
        FROM jobs
        GROUP BY sector, emirate, seniority_band, remote;
    """,

    # ── PAGE 2: Salary Intelligence ───────────────────────────────────────────

    # FIX: salary_source added to SELECT and GROUP BY so the Page 2 slicer
    # ("filter to reported-only") can bind directly to this view.
    "vw_salary_bands": """
        CREATE OR REPLACE VIEW vw_salary_bands AS
        SELECT
            title_normalized,
            emirate,
            sector,
            seniority_band,
            salary_source,
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

    # NEW: was in kpi_views.sql but completely absent from load_db.py
    "vw_skill_salary_premium": """
        CREATE OR REPLACE VIEW vw_skill_salary_premium AS
        WITH baseline AS (
            SELECT AVG(salary_aed_monthly) AS overall_avg
            FROM jobs
            WHERE salary_aed_monthly BETWEEN 2000 AND 200000
        ),
        skill_salaries AS (
            SELECT
                js.skill,
                js.sector,
                js.emirate,
                ROUND(AVG(j.salary_aed_monthly))  AS avg_salary_aed,
                COUNT(*)                          AS job_count
            FROM job_skills js
            JOIN jobs j ON js.job_id = j.job_id
            WHERE j.salary_aed_monthly BETWEEN 2000 AND 200000
            GROUP BY js.skill, js.sector, js.emirate
            HAVING COUNT(*) >= 3
        )
        SELECT
            ss.skill,
            ss.sector,
            ss.emirate,
            ss.avg_salary_aed,
            ss.job_count,
            ROUND((ss.avg_salary_aed - b.overall_avg) / b.overall_avg * 100, 1)
                                                  AS salary_premium_pct
        FROM skill_salaries ss
        CROSS JOIN baseline b
        ORDER BY salary_premium_pct DESC;
    """,

    # ── PAGE 3: Skills Intelligence ───────────────────────────────────────────

    # FIX: job_id added to SELECT and GROUP BY — required for the Power BI
    # model relationship vw_jobs_detail[job_id] → vw_skill_heatmap[job_id].
    "vw_skill_heatmap": """
        CREATE OR REPLACE VIEW vw_skill_heatmap AS
        SELECT
            job_id,
            skill,
            emirate,
            sector,
            seniority_band,
            COUNT(*)                                                        AS count,
            ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed
        FROM job_skills
        WHERE skill IS NOT NULL AND skill != ''
        GROUP BY job_id, skill, emirate, sector, seniority_band;
    """,

    # NEW: was in kpi_views.sql but completely absent from load_db.py
    "vw_skill_cooccurrence": """
        CREATE OR REPLACE VIEW vw_skill_cooccurrence AS
        SELECT
            a.skill                  AS skill_a,
            b.skill                  AS skill_b,
            COUNT(*)                 AS cooccurrence_count,
            COUNT(DISTINCT a.emirate) AS emirate_spread
        FROM job_skills a
        JOIN job_skills b
            ON  a.job_id = b.job_id
            AND a.skill  < b.skill
        WHERE a.skill IS NOT NULL AND b.skill IS NOT NULL
        GROUP BY a.skill, b.skill
        HAVING COUNT(*) >= 5
        ORDER BY cooccurrence_count DESC
        LIMIT 200;
    """,

    # NEW: was in kpi_views.sql but completely absent from load_db.py
    "vw_emerging_skills": """
        CREATE OR REPLACE VIEW vw_emerging_skills AS
        WITH current_period AS (
            SELECT skill, COUNT(*) AS current_count
            FROM job_skills
            WHERE posted_at >= NOW() - INTERVAL '30 days'
            GROUP BY skill
        ),
        prior_period AS (
            SELECT skill, COUNT(*) AS prior_count
            FROM job_skills
            WHERE posted_at BETWEEN NOW() - INTERVAL '60 days'
                                AND NOW() - INTERVAL '30 days'
            GROUP BY skill
        )
        SELECT
            c.skill,
            c.current_count,
            COALESCE(p.prior_count, 0)                              AS prior_count,
            c.current_count - COALESCE(p.prior_count, 0)           AS absolute_growth,
            CASE
                WHEN COALESCE(p.prior_count, 0) = 0 THEN NULL
                ELSE ROUND(100.0 * (c.current_count - p.prior_count) / p.prior_count, 1)
            END                                                     AS growth_pct
        FROM current_period c
        LEFT JOIN prior_period p USING (skill)
        WHERE c.current_count >= 5
        ORDER BY absolute_growth DESC;
    """,

    # ── PAGE 4: Company Intelligence ──────────────────────────────────────────

    # FIX: hiring_intensity was missing from the original load_db.py version
    "vw_company_radar": """
        CREATE OR REPLACE VIEW vw_company_radar AS
        SELECT
            j.company,
            j.emirate,
            j.sector,
            j.company_size_band,
            j.is_mnc,
            j.company_hq_country,
            COUNT(*)                                                        AS open_roles,
            ROUND(AVG(j.salary_aed_monthly) FILTER (WHERE j.salary_aed_monthly > 0)) AS avg_salary_aed,
            COUNT(DISTINCT DATE_TRUNC('week', j.posted_at))                 AS active_weeks,
            ROUND(AVG(j.glassdoor_rating) FILTER (WHERE j.glassdoor_rating IS NOT NULL), 1)
                                                                            AS avg_glassdoor_rating,
            ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT DATE_TRUNC('week', j.posted_at)), 0), 1)
                                                                            AS hiring_intensity
        FROM jobs j
        WHERE j.company IS NOT NULL AND j.company != ''
        GROUP BY j.company, j.emirate, j.sector,
                 j.company_size_band, j.is_mnc, j.company_hq_country
        ORDER BY open_roles DESC
        LIMIT 100;
    """,

    # NEW: was in kpi_views.sql but completely absent from load_db.py
    "vw_mnc_vs_local": """
        CREATE OR REPLACE VIEW vw_mnc_vs_local AS
        SELECT
            CASE WHEN is_mnc THEN 'MNC' ELSE 'Local / Regional' END AS company_type,
            sector,
            emirate,
            COUNT(*)                                                 AS job_count,
            ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed,
            ROUND(AVG(skills_count), 1)                              AS avg_skills_required
        FROM jobs
        WHERE is_mnc IS NOT NULL
        GROUP BY 1, 2, 3;
    """,

    # ── PAGE 5 & 6: Data Quality & Pipeline Health ────────────────────────────

    "vw_pipeline_health": """
        CREATE OR REPLACE VIEW vw_pipeline_health AS
        SELECT
            COUNT(*)                                                               AS total_jobs,
            COUNT(*) FILTER (WHERE source = 'jsearch')                             AS jsearch_jobs,
            COUNT(*) FILTER (WHERE source = 'adzuna')                              AS adzuna_jobs,
            ROUND(100.0 * COUNT(*) FILTER (WHERE salary_aed_monthly > 0)
                  / NULLIF(COUNT(*), 0), 1)                                        AS pct_salary_filled,
            ROUND(100.0 * COUNT(*) FILTER (WHERE salary_source = 'reported')
                  / NULLIF(COUNT(*), 0), 1)                                        AS pct_salary_reported,
            ROUND(100.0 * COUNT(*) FILTER (WHERE salary_source LIKE 'imputed%')
                  / NULLIF(COUNT(*), 0), 1)                                        AS pct_salary_imputed,
            ROUND(AVG(completeness_score), 1)                                      AS avg_completeness_score,
            ROUND(AVG(skills_count), 1)                                            AS avg_skills_per_job,
            MAX(posted_at)                                                         AS newest_posting,
            MIN(posted_at)                                                         AS oldest_posting,
            -- FIX: use CURRENT_DATE arithmetic — days_since_posted column may lag
            -- by one day if the ETL ran before midnight; computing on-the-fly is safer
            ROUND(AVG(CURRENT_DATE - posted_at::date), 0)                          AS avg_days_since_posted,
            COUNT(DISTINCT company)                                                AS unique_companies,
            COUNT(DISTINCT emirate)                                                AS unique_emirates
        FROM jobs;
    """,

    # NEW: was in kpi_views.sql but completely absent from load_db.py
    "vw_field_coverage": """
        CREATE OR REPLACE VIEW vw_field_coverage AS
        SELECT 'title'             AS field,
               ROUND(100.0 * COUNT(*) FILTER (WHERE title IS NOT NULL AND title != '') / COUNT(*), 1)
                                   AS pct_filled FROM jobs
        UNION ALL
        SELECT 'company',
               ROUND(100.0 * COUNT(*) FILTER (WHERE company IS NOT NULL AND company != '') / COUNT(*), 1)
                                   FROM jobs
        UNION ALL
        SELECT 'emirate',
               ROUND(100.0 * COUNT(*) FILTER (WHERE emirate IS NOT NULL) / COUNT(*), 1)
                                   FROM jobs
        UNION ALL
        SELECT 'sector',
               ROUND(100.0 * COUNT(*) FILTER (WHERE sector != 'Other') / COUNT(*), 1)
                                   FROM jobs
        UNION ALL
        SELECT 'salary_aed_monthly',
               ROUND(100.0 * COUNT(*) FILTER (WHERE salary_aed_monthly > 0) / COUNT(*), 1)
                                   FROM jobs
        UNION ALL
        SELECT 'salary_reported',
               ROUND(100.0 * COUNT(*) FILTER (WHERE salary_source = 'reported') / COUNT(*), 1)
                                   FROM jobs
        UNION ALL
        SELECT 'seniority_band',
               ROUND(100.0 * COUNT(*) FILTER (WHERE seniority_band != 'Mid-Level') / COUNT(*), 1)
                                   FROM jobs
        UNION ALL
        SELECT 'company_size_band',
               ROUND(100.0 * COUNT(*) FILTER (WHERE company_size_band IS NOT NULL
                                                AND company_size_band != 'Unknown') / COUNT(*), 1)
                                   FROM jobs
        UNION ALL
        SELECT 'posted_at',
               ROUND(100.0 * COUNT(*) FILTER (WHERE posted_at IS NOT NULL) / COUNT(*), 1)
                                   FROM jobs
        UNION ALL
        SELECT 'skills_count >= 1',
               ROUND(100.0 * COUNT(*) FILTER (WHERE skills_count >= 1) / COUNT(*), 1)
                                   FROM jobs;
    """,

    # NEW: was in kpi_views.sql but completely absent from load_db.py
    "vw_pipeline_runs": """
        CREATE OR REPLACE VIEW vw_pipeline_runs AS
        SELECT
            id,
            run_at,
            db_target,
            jobs_total,
            jobs_inserted,
            skill_tags,
            ROUND(salary_coverage, 1)   AS salary_coverage_pct,
            ROUND(avg_completeness, 1)  AS avg_completeness_score,
            status,
            notes
        FROM pipeline_runs
        ORDER BY run_at DESC;
    """,

    # ── PAGE 6: Job Explorer (drill-through) ──────────────────────────────────

    # FIX: days_since_posted added — computed live so it stays accurate without
    # needing a daily ETL refresh of the stored column.
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
            apply_link
        FROM jobs
        ORDER BY posted_at DESC NULLS LAST;
    """,
}


# ─────────────────────────────────────────────────────────────────────────────
# Load functions
# ─────────────────────────────────────────────────────────────────────────────

def create_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(SCHEMA_DDL))
    print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Tables and indexes ready")


def load_jobs(engine, df: pd.DataFrame) -> int:
    """Upserts jobs — inserts new, skips existing (ON CONFLICT DO NOTHING)."""
    cols = [c for c in df.columns if c in [
        "job_id", "source", "title", "title_normalized", "company", "city",
        "emirate", "sector", "seniority_band", "employment_type", "remote",
        "salary_min", "salary_max", "salary_currency", "salary_aed_monthly", "salary_source",
        "company_size_band", "is_mnc", "company_hq_country", "company_industry",
        "glassdoor_rating", "enrichment_source",
        "skills_count", "completeness_score", "days_since_posted",
        "posted_at", "apply_link",
    ]]

    inserted = 0
    with engine.begin() as conn:
        for _, row in df[cols].iterrows():
            row_dict = row.where(pd.notnull(row), None).to_dict()
            # Cast bool columns
            for bool_col in ["remote", "is_mnc"]:
                if bool_col in row_dict and row_dict[bool_col] is not None:
                    row_dict[bool_col] = bool(row_dict[bool_col])
            try:
                result = conn.execute(
                    text("""
                        INSERT INTO jobs ({cols})
                        VALUES ({placeholders})
                        ON CONFLICT (job_id) DO NOTHING
                    """.format(
                        cols=", ".join(cols),
                        placeholders=", ".join(f":{c}" for c in cols),
                    )),
                    row_dict,
                )
                inserted += result.rowcount
            except Exception as exc:
                jid = row_dict.get("job_id", "?")
                print(f"    {Fore.YELLOW}⚠{Style.RESET_ALL} Skipping {jid}: {exc}")
    return inserted


def load_skills(engine, df: pd.DataFrame) -> int:
    """Replaces skill tags for affected job_ids (delete + insert)."""
    if df.empty:
        return 0

    skill_cols = [c for c in df.columns if c in [
        "job_id", "title", "title_normalized", "company", "emirate",
        "sector", "seniority_band", "skill", "salary_aed_monthly",
        "salary_source", "posted_at",
    ]]
    df_out = df[skill_cols].copy()

    job_ids = df_out["job_id"].dropna().unique().tolist()
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM job_skills WHERE job_id = ANY(:ids)"),
            {"ids": job_ids},
        )
        df_out.to_sql("job_skills", conn, if_exists="append", index=False)
    return len(df_out)


def create_views(engine) -> None:
    with engine.begin() as conn:
        for name, ddl in VIEWS_DDL.items():
            try:
                conn.execute(text(ddl))
                print(f"    {Fore.GREEN}✓{Style.RESET_ALL} {name}")
            except Exception as exc:
                print(f"    {Fore.YELLOW}⚠{Style.RESET_ALL} {name}: {exc}")


def log_pipeline_run(engine, meta: dict, n_jobs: int, n_skills: int, status: str = "success") -> None:
    """Writes a row to pipeline_runs for the Data Quality page in Power BI."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO pipeline_runs
                    (db_target, jobs_total, jobs_inserted, skill_tags,
                     salary_coverage, avg_completeness, status)
                VALUES
                    (:db_target, :jobs_total, :jobs_inserted, :skill_tags,
                     :salary_coverage, :avg_completeness, :status)
            """),
            {
                "db_target":        os.getenv("DB_TARGET", "local"),
                "jobs_total":       meta.get("total_jobs", 0),
                "jobs_inserted":    n_jobs,
                "skill_tags":       n_skills,
                "salary_coverage":  meta.get("salary_coverage_pct", 0),
                "avg_completeness": meta.get("avg_completeness", 0),
                "status":           status,
            },
        )


# ─────────────────────────────────────────────────────────────────────────────
# Main runner
# ─────────────────────────────────────────────────────────────────────────────

def run_load():
    engine = get_engine()

    print(f"  {Fore.CYAN}Creating/verifying tables...{Style.RESET_ALL}")
    create_tables(engine)

    print(f"  {Fore.CYAN}Reading processed CSVs...{Style.RESET_ALL}")
    df_jobs   = pd.read_csv("data/processed/jobs_clean.csv",  low_memory=False)
    df_skills = pd.read_csv("data/processed/skills_long.csv", low_memory=False)

    # Load transform metadata for pipeline_runs log
    meta = {}
    meta_path = Path("data/processed/transform_meta.json")
    if meta_path.exists():
        with open(meta_path) as f:
            meta = json.load(f)

    print(f"  {Fore.CYAN}Loading jobs...{Style.RESET_ALL}")
    n_jobs = load_jobs(engine, df_jobs)
    print(f"    → {n_jobs:,} new rows inserted ({len(df_jobs):,} processed)")

    print(f"  {Fore.CYAN}Loading skill tags...{Style.RESET_ALL}")
    n_skills = load_skills(engine, df_skills)
    print(f"    → {n_skills:,} skill tag rows")

    print(f"  {Fore.CYAN}Creating KPI views...{Style.RESET_ALL}")
    create_views(engine)

    print(f"  {Fore.CYAN}Logging pipeline run...{Style.RESET_ALL}")
    log_pipeline_run(engine, meta, n_jobs, n_skills)

    print(f"\n  {Fore.GREEN}✅ Load complete{Style.RESET_ALL}")


if __name__ == "__main__":
    run_load()