"""
load_db.py
Reads cleaned CSVs from data/processed/ and upserts into the target database.
Target is controlled by DB_TARGET in .env ("local" or "supabase").
Idempotent — safe to run multiple times.
"""

import os
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv
from etl.db import get_engine   # ← single import change vs original

load_dotenv()


def create_tables(engine):
    """Create tables and indexes if they don't exist yet."""
    ddl = """
    CREATE TABLE IF NOT EXISTS jobs (
        job_id              TEXT PRIMARY KEY,
        source              TEXT,
        title               TEXT,
        company             TEXT,
        city                TEXT,
        emirate             TEXT,
        sector              TEXT,
        employment_type     TEXT,
        remote              BOOLEAN,
        salary_min          NUMERIC,
        salary_max          NUMERIC,
        salary_currency     TEXT,
        salary_aed_monthly  NUMERIC,
        skills_count        INTEGER,
        posted_at           TIMESTAMP,
        apply_link          TEXT,
        fetched_at          TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS job_skills (
        id          SERIAL PRIMARY KEY,
        job_id      TEXT,
        title       TEXT,
        company     TEXT,
        emirate     TEXT,
        sector      TEXT,
        skill       TEXT,
        posted_at   TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_jobs_emirate   ON jobs(emirate);
    CREATE INDEX IF NOT EXISTS idx_jobs_sector    ON jobs(sector);
    CREATE INDEX IF NOT EXISTS idx_jobs_posted    ON jobs(posted_at);
    CREATE INDEX IF NOT EXISTS idx_skills_skill   ON job_skills(skill);
    CREATE INDEX IF NOT EXISTS idx_skills_emirate ON job_skills(emirate);
    CREATE INDEX IF NOT EXISTS idx_skills_sector  ON job_skills(sector);
    CREATE INDEX IF NOT EXISTS idx_skills_job     ON job_skills(job_id);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("  Tables ready.")


def load_jobs(engine, df: pd.DataFrame) -> int:
    inserted = 0
    cols = [
        "job_id", "source", "title", "company", "city", "emirate", "sector",
        "employment_type", "remote", "salary_min", "salary_max",
        "salary_currency", "salary_aed_monthly", "skills_count",
        "posted_at", "apply_link",
    ]
    cols = [c for c in cols if c in df.columns]

    with engine.begin() as conn:
        for _, row in df[cols].iterrows():
            row_dict = row.where(pd.notnull(row), None).to_dict()
            try:
                result = conn.execute(text("""
                    INSERT INTO jobs ({cols})
                    VALUES ({placeholders})
                    ON CONFLICT (job_id) DO NOTHING
                """.format(
                    cols=", ".join(cols),
                    placeholders=", ".join(f":{c}" for c in cols)
                )), row_dict)
                inserted += result.rowcount
            except Exception as e:
                print(f"    ⚠  Skipping job {row_dict.get('job_id')}: {e}")
    return inserted


def load_skills(engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    job_ids = df["job_id"].unique().tolist()
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM job_skills WHERE job_id = ANY(:ids)"),
            {"ids": job_ids}
        )
        df.to_sql("job_skills", conn, if_exists="append", index=False)
    return len(df)


def create_views(engine):
    views = {
        "vw_top_skills": """
            CREATE OR REPLACE VIEW vw_top_skills AS
            SELECT skill,
                   COUNT(*)                AS demand_count,
                   COUNT(DISTINCT emirate) AS emirate_spread,
                   COUNT(DISTINCT sector)  AS sector_spread
            FROM job_skills
            WHERE skill IS NOT NULL AND skill != ''
            GROUP BY skill ORDER BY demand_count DESC LIMIT 30;
        """,
        "vw_salary_bands": """
            CREATE OR REPLACE VIEW vw_salary_bands AS
            SELECT title, emirate, sector,
                   ROUND(AVG(salary_aed_monthly)) AS avg_salary_aed,
                   ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS p25_aed,
                   ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS p75_aed,
                   COUNT(*) AS sample_size
            FROM jobs
            WHERE salary_aed_monthly BETWEEN 1000 AND 200000
            GROUP BY title, emirate, sector HAVING COUNT(*) >= 2;
        """,
        "vw_hiring_velocity": """
            CREATE OR REPLACE VIEW vw_hiring_velocity AS
            SELECT DATE_TRUNC('week', posted_at) AS week_start,
                   emirate, sector, COUNT(*) AS jobs_posted
            FROM jobs WHERE posted_at IS NOT NULL
            GROUP BY 1, 2, 3 ORDER BY 1 DESC;
        """,
        "vw_skill_heatmap": """
            CREATE OR REPLACE VIEW vw_skill_heatmap AS
            SELECT skill, emirate, sector, COUNT(*) AS count
            FROM job_skills
            WHERE emirate NOT IN ('Other UAE','Unknown') AND skill IS NOT NULL
            GROUP BY skill, emirate, sector;
        """,
        "vw_company_radar": """
            CREATE OR REPLACE VIEW vw_company_radar AS
            SELECT company, emirate, sector,
                   COUNT(*) AS open_roles,
                   ROUND(AVG(salary_aed_monthly)) AS avg_salary_aed,
                   COUNT(DISTINCT DATE_TRUNC('week', posted_at)) AS active_weeks
            FROM jobs WHERE company IS NOT NULL AND company != ''
            GROUP BY company, emirate, sector ORDER BY open_roles DESC LIMIT 50;
        """,
        "vw_remote_analysis": """
            CREATE OR REPLACE VIEW vw_remote_analysis AS
            SELECT sector, emirate, remote,
                   COUNT(*) AS job_count,
                   ROUND(AVG(salary_aed_monthly)) AS avg_salary_aed
            FROM jobs GROUP BY sector, emirate, remote;
        """,
        "vw_jobs_detail": """
            CREATE OR REPLACE VIEW vw_jobs_detail AS
            SELECT job_id, source, title, company, emirate, sector,
                   employment_type, remote, salary_aed_monthly,
                   skills_count, posted_at, apply_link
            FROM jobs ORDER BY posted_at DESC;
        """,
    }
    with engine.begin() as conn:
        for name, ddl in views.items():
            try:
                conn.execute(text(ddl))
                print(f"    ✓ {name}")
            except Exception as e:
                print(f"    ⚠  {name}: {e}")


def run_load():
    engine = get_engine()   # reads DB_TARGET from .env automatically

    print("  Creating/verifying tables...")
    create_tables(engine)

    print("  Loading data...")
    df_jobs   = pd.read_csv("data/processed/jobs_clean.csv")
    df_skills = pd.read_csv("data/processed/skills_long.csv")

    n_jobs   = load_jobs(engine, df_jobs)
    n_skills = load_skills(engine, df_skills)

    print(f"    → Jobs inserted : {n_jobs}")
    print(f"    → Skill tags    : {n_skills}")

    print("  Creating KPI views...")
    create_views(engine)

    print("\n✅ Load complete.")


if __name__ == "__main__":
    run_load()