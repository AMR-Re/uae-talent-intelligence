-- schema.sql
-- Run this manually once before first pipeline run
-- psql -U postgres -d uae_talent -f sql/schema.sql

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

-- Indexes for fast Power BI queries
CREATE INDEX IF NOT EXISTS idx_jobs_emirate   ON jobs(emirate);
CREATE INDEX IF NOT EXISTS idx_jobs_sector    ON jobs(sector);
CREATE INDEX IF NOT EXISTS idx_jobs_posted    ON jobs(posted_at);
CREATE INDEX IF NOT EXISTS idx_skills_skill   ON job_skills(skill);
CREATE INDEX IF NOT EXISTS idx_skills_emirate ON job_skills(emirate);
CREATE INDEX IF NOT EXISTS idx_skills_sector  ON job_skills(sector);
CREATE INDEX IF NOT EXISTS idx_skills_job     ON job_skills(job_id);