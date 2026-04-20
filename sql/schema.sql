-- ─────────────────────────────────────────────────────────────────────────────
-- schema.sql
-- UAE Talent Intelligence — Master database schema
--
-- Run manually once before the first pipeline run:
--   psql -U postgres -c "CREATE DATABASE uae_talent;"
--   psql -U postgres -d uae_talent -f sql/schema.sql
--
-- The ETL pipeline (load_db.py) also runs CREATE IF NOT EXISTS on every run,
-- so you only need this file for the initial setup or to inspect the schema.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Core jobs table ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS jobs (
    job_id               TEXT PRIMARY KEY,
    source               TEXT,               -- 'jsearch' | 'adzuna'
    title                TEXT,               -- raw title from API
    title_normalized     TEXT,               -- standardised title bucket
    company              TEXT,
    city                 TEXT,               -- raw city string from API
    emirate              TEXT,               -- Dubai | Abu Dhabi | Sharjah | ...
    sector               TEXT,               -- Technology | Finance | HR | ...
    seniority_band       TEXT,               -- C-Suite | Director+ | Manager | Senior | Mid-Level | Junior
    employment_type      TEXT,               -- FULLTIME | PARTTIME | CONTRACT | INTERN
    remote               BOOLEAN,

    -- Salary (raw + normalised)
    salary_min           NUMERIC,
    salary_max           NUMERIC,
    salary_currency      TEXT,
    salary_aed_monthly   NUMERIC,            -- normalised midpoint in AED/month
    salary_source        TEXT,               -- 'reported' | 'imputed_peer_L1..L4' | 'imputed_model' | 'imputed_benchmark'

    -- Company enrichment
    company_size_band    TEXT,               -- '1-50' | '51-200' | '201-1000' | '1000+'
    is_mnc               BOOLEAN,            -- multinational flag
    company_hq_country   TEXT,
    company_industry     TEXT,               -- finer-grained than sector
    glassdoor_rating     NUMERIC,
    enrichment_source    TEXT,               -- how company data was obtained

    -- Quality signals
    skills_count         INTEGER,            -- # skills extracted from description
    completeness_score   INTEGER,            -- 0–100 data quality score
    days_since_posted    INTEGER,

    posted_at            TIMESTAMP WITH TIME ZONE,
    apply_link           TEXT,
    fetched_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- ── Skill tags (long/exploded form) ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS job_skills (
    id                   SERIAL PRIMARY KEY,
    job_id               TEXT REFERENCES jobs(job_id) ON DELETE CASCADE,
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


-- ── Pipeline audit log ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id               SERIAL PRIMARY KEY,
    run_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    db_target        TEXT,
    jobs_total       INTEGER,
    jobs_inserted    INTEGER,
    skill_tags       INTEGER,
    salary_coverage  NUMERIC,    -- % of jobs with salary_aed_monthly > 0
    avg_completeness NUMERIC,    -- average completeness_score
    status           TEXT,       -- 'success' | 'partial' | 'failed'
    notes            TEXT
);


-- ── Indexes ────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_jobs_emirate         ON jobs(emirate);
CREATE INDEX IF NOT EXISTS idx_jobs_sector          ON jobs(sector);
CREATE INDEX IF NOT EXISTS idx_jobs_seniority       ON jobs(seniority_band);
CREATE INDEX IF NOT EXISTS idx_jobs_posted          ON jobs(posted_at);
CREATE INDEX IF NOT EXISTS idx_jobs_salary          ON jobs(salary_aed_monthly);
CREATE INDEX IF NOT EXISTS idx_jobs_salary_source   ON jobs(salary_source);
CREATE INDEX IF NOT EXISTS idx_jobs_completeness    ON jobs(completeness_score);
CREATE INDEX IF NOT EXISTS idx_jobs_title_norm      ON jobs(title_normalized);
CREATE INDEX IF NOT EXISTS idx_jobs_company_size    ON jobs(company_size_band);
CREATE INDEX IF NOT EXISTS idx_jobs_is_mnc          ON jobs(is_mnc);

CREATE INDEX IF NOT EXISTS idx_skills_skill         ON job_skills(skill);
CREATE INDEX IF NOT EXISTS idx_skills_emirate       ON job_skills(emirate);
CREATE INDEX IF NOT EXISTS idx_skills_sector        ON job_skills(sector);
CREATE INDEX IF NOT EXISTS idx_skills_seniority     ON job_skills(seniority_band);
CREATE INDEX IF NOT EXISTS idx_skills_job           ON job_skills(job_id);
CREATE INDEX IF NOT EXISTS idx_skills_posted        ON job_skills(posted_at);