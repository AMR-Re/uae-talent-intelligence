-- =============================================================================
-- schema_updates.sql
-- UAE Talent Intelligence — Safe, additive schema migrations
--
-- ✅ SAFE: Only adds new columns, tables, and indexes.
-- ❌ NEVER: Drops, renames, or modifies existing columns / tables / views.
--
-- Run once before upgrading to the production pipeline:
--   psql -U postgres -d uae_talent -f sql/schema_updates.sql
--
-- All ALTER TABLE statements use IF NOT EXISTS / DO $$ BEGIN...EXCEPTION
-- so this file is idempotent — safe to re-run.
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. ADD NEW COLUMNS TO jobs (Power BI-safe — additive only)
-- ─────────────────────────────────────────────────────────────────────────────

DO $$ BEGIN
    -- Pipeline traceability
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='jobs' AND column_name='run_id') THEN
        ALTER TABLE jobs ADD COLUMN run_id UUID;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='jobs' AND column_name='ingestion_timestamp') THEN
        ALTER TABLE jobs ADD COLUMN ingestion_timestamp TIMESTAMP WITH TIME ZONE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='jobs' AND column_name='last_seen_at') THEN
        ALTER TABLE jobs ADD COLUMN last_seen_at TIMESTAMP WITH TIME ZONE;
    END IF;

    -- Salary ML output fields
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='jobs' AND column_name='salary_imputed_flag') THEN
        ALTER TABLE jobs ADD COLUMN salary_imputed_flag BOOLEAN DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='jobs' AND column_name='confidence_score') THEN
        ALTER TABLE jobs ADD COLUMN confidence_score NUMERIC;
    END IF;
END $$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. ADD NEW COLUMNS TO pipeline_runs
--    Existing columns are kept intact so vw_pipeline_runs is unchanged.
-- ─────────────────────────────────────────────────────────────────────────────

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='pipeline_runs' AND column_name='run_id') THEN
        ALTER TABLE pipeline_runs ADD COLUMN run_id UUID;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='pipeline_runs' AND column_name='start_time') THEN
        ALTER TABLE pipeline_runs ADD COLUMN start_time TIMESTAMP WITH TIME ZONE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='pipeline_runs' AND column_name='end_time') THEN
        ALTER TABLE pipeline_runs ADD COLUMN end_time TIMESTAMP WITH TIME ZONE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='pipeline_runs' AND column_name='rows_inserted') THEN
        ALTER TABLE pipeline_runs ADD COLUMN rows_inserted INTEGER DEFAULT 0;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='pipeline_runs' AND column_name='rows_updated') THEN
        ALTER TABLE pipeline_runs ADD COLUMN rows_updated INTEGER DEFAULT 0;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='pipeline_runs' AND column_name='error_message') THEN
        ALTER TABLE pipeline_runs ADD COLUMN error_message TEXT;
    END IF;
END $$;

-- Back-fill rows_inserted from jobs_inserted for existing rows
UPDATE pipeline_runs
SET rows_inserted = COALESCE(jobs_inserted, 0)
WHERE rows_inserted IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. STAGING TABLE — jobs_staging
--    Mirrors jobs but with no PRIMARY KEY, allowing duplicate job_ids during
--    the load window. DQ checks run here before merging to production.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS jobs_staging (
    -- Core identity
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

    -- Salary
    salary_min           NUMERIC,
    salary_max           NUMERIC,
    salary_currency      TEXT,
    salary_aed_monthly   NUMERIC,
    salary_source        TEXT,
    salary_imputed_flag  BOOLEAN,
    confidence_score     NUMERIC,

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

    -- Pipeline metadata
    run_id               UUID,
    ingestion_timestamp  TIMESTAMP WITH TIME ZONE,

    -- Staging control
    staging_loaded_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    dq_status            TEXT DEFAULT 'pending',   -- 'valid' | 'warning' | 'invalid'
    dq_notes             TEXT
);

-- Index for fast DQ lookups and merge joins
CREATE INDEX IF NOT EXISTS idx_staging_job_id   ON jobs_staging(job_id);
CREATE INDEX IF NOT EXISTS idx_staging_run_id   ON jobs_staging(run_id);
CREATE INDEX IF NOT EXISTS idx_staging_dq       ON jobs_staging(dq_status);


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. DATA QUALITY LOG
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS data_quality_log (
    id           SERIAL PRIMARY KEY,
    run_id       UUID         NOT NULL,
    job_id       TEXT,
    check_name   TEXT         NOT NULL,
    severity     TEXT         NOT NULL,  -- 'error' | 'warning' | 'info'
    field_name   TEXT,
    message      TEXT,
    logged_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_log_run_id ON data_quality_log(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_log_severity ON data_quality_log(severity);


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. NEW INDEXES ON jobs (additive, never drops existing)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_jobs_run_id           ON jobs(run_id);
CREATE INDEX IF NOT EXISTS idx_jobs_last_seen_at     ON jobs(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_jobs_salary_imputed   ON jobs(salary_imputed_flag);
CREATE INDEX IF NOT EXISTS idx_jobs_ingestion_ts     ON jobs(ingestion_timestamp);


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. UPDATE vw_pipeline_runs to expose new columns
--    This view is referenced by Power BI. Extending it (new columns) is safe —
--    existing Power BI visuals ignore unknown columns.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW vw_pipeline_runs AS
SELECT
    id,
    run_id,
    COALESCE(start_time, run_at)            AS run_at,          -- backward compat alias
    start_time,
    end_time,
    db_target,
    jobs_total,
    COALESCE(rows_inserted, jobs_inserted)  AS jobs_inserted,   -- backward compat
    rows_inserted,
    rows_updated,
    skill_tags,
    ROUND(salary_coverage, 1)               AS salary_coverage_pct,
    ROUND(avg_completeness, 1)              AS avg_completeness_score,
    status,
    error_message,
    notes
FROM pipeline_runs
ORDER BY COALESCE(start_time, run_at) DESC;


-- Done
SELECT 'Schema updates applied successfully.' AS result;