-- ─────────────────────────────────────────────────────────────────────────────
-- kpi_views.sql
-- UAE Talent Intelligence — All Power BI views
--
-- Power BI connects to these views via DirectQuery or Import mode.
-- run_load.py creates these automatically on every pipeline run.
-- You can also run this file manually:
--   psql -U postgres -d uae_talent -f sql/kpi_views.sql
-- ─────────────────────────────────────────────────────────────────────────────


-- ════════════════════════════════════════════════════════════════════════════
-- PAGE 1: Market Overview
-- ════════════════════════════════════════════════════════════════════════════

-- ── vw_top_skills ─────────────────────────────────────────────────────────────
-- Powers: Ranked bar chart — Top Skills in UAE Job Market
-- Trend arrows: demand_last_30d vs demand_prior_30d
-- Salary premium: avg_salary_aed per skill (use in tooltip)
-- FIX: Added top_emirate so the tooltip page (Page 7) has a real emirate value
--      instead of the integer emirate_spread column.

CREATE OR REPLACE VIEW vw_top_skills AS
WITH skill_base AS (
    SELECT
        skill,
        emirate,
        posted_at,
        salary_aed_monthly
    FROM job_skills
    WHERE skill IS NOT NULL AND skill != ''
),
top_emirate AS (
    -- Most common emirate per skill — used by the tooltip page
    SELECT DISTINCT ON (skill)
        skill,
        emirate AS top_emirate
    FROM skill_base
    WHERE emirate IS NOT NULL
    GROUP BY skill, emirate
    ORDER BY skill, COUNT(*) DESC
)
SELECT
    sb.skill,
    COUNT(*)                                                        AS demand_count,
    COUNT(DISTINCT sb.emirate)                                      AS emirate_spread,
    COUNT(DISTINCT sector)                                          AS sector_spread,
    COUNT(DISTINCT seniority_band)                                  AS seniority_spread,
    ROUND(AVG(sb.salary_aed_monthly) FILTER (WHERE sb.salary_aed_monthly > 0)) AS avg_salary_aed,

    -- Trend calculation (current 30 days vs prior 30 days)
    COUNT(*) FILTER (WHERE sb.posted_at >= NOW() - INTERVAL '30 days')  AS demand_last_30d,
    COUNT(*) FILTER (
        WHERE sb.posted_at BETWEEN NOW() - INTERVAL '60 days'
                        AND     NOW() - INTERVAL '30 days'
    )                                                               AS demand_prior_30d,

    -- Derived trend direction for Power BI conditional formatting
    CASE
        WHEN COUNT(*) FILTER (WHERE sb.posted_at >= NOW() - INTERVAL '30 days') >
             COUNT(*) FILTER (
                WHERE sb.posted_at BETWEEN NOW() - INTERVAL '60 days'
                                AND     NOW() - INTERVAL '30 days'
             ) THEN 'Rising'
        WHEN COUNT(*) FILTER (WHERE sb.posted_at >= NOW() - INTERVAL '30 days') <
             COUNT(*) FILTER (
                WHERE sb.posted_at BETWEEN NOW() - INTERVAL '60 days'
                                AND     NOW() - INTERVAL '30 days'
             ) THEN 'Falling'
        ELSE 'Stable'
    END AS trend_direction,

    -- FIX: top_emirate — the most common emirate for this skill.
    -- Used by the tooltip page (Page 7) so it has a real text emirate value
    -- instead of the integer emirate_spread. Bind the tooltip page's emirate
    -- field to this column.
    te.top_emirate AS emirate

FROM skill_base sb
LEFT JOIN top_emirate te USING (skill)
GROUP BY sb.skill, te.top_emirate
ORDER BY demand_count DESC
LIMIT 50;


-- ── vw_hiring_velocity ────────────────────────────────────────────────────────
-- Powers: Line chart — Weekly Hiring Pulse
-- Slicers: emirate, sector

CREATE OR REPLACE VIEW vw_hiring_velocity AS
SELECT
    DATE_TRUNC('week', posted_at)                                   AS week_start,
    emirate,
    sector,
    seniority_band,
    COUNT(*)                                                        AS jobs_posted,
    ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed,

    -- 4-week rolling average for trend line (use as secondary Y axis)
    ROUND(AVG(COUNT(*)) OVER (
        PARTITION BY emirate, sector
        ORDER BY DATE_TRUNC('week', posted_at)
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ), 0)                                                           AS rolling_4w_avg

FROM jobs
WHERE posted_at IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC;


-- ── vw_remote_analysis ────────────────────────────────────────────────────────
-- Powers: Stacked column — Remote vs On-Site by sector

CREATE OR REPLACE VIEW vw_remote_analysis AS
SELECT
    sector,
    emirate,
    seniority_band,
    remote,
    COUNT(*)                                                        AS job_count,
    ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY sector, emirate), 1)
                                                                    AS pct_of_sector_emirate
FROM jobs
GROUP BY sector, emirate, seniority_band, remote;


-- ════════════════════════════════════════════════════════════════════════════
-- PAGE 2: Salary Intelligence
-- ════════════════════════════════════════════════════════════════════════════

-- ── vw_salary_bands ───────────────────────────────────────────────────────────
-- Powers: Range bars, salary benchmarking table
-- FIX: Added salary_source column so the Page 2 slicer can filter directly
--      on this view (reported / imputed_benchmark / imputed_model etc.)
--      The view now outputs one row per title+emirate+sector+seniority+source
--      so Power BI can slice by source while keeping all other dimensions.

CREATE OR REPLACE VIEW vw_salary_bands AS
SELECT
    title_normalized,
    emirate,
    sector,
    seniority_band,
    salary_source,                                                          -- FIX: exposed for Page 2 slicer
    ROUND(AVG(salary_aed_monthly))                                          AS avg_salary_aed,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS p25_aed,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS median_aed,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS p75_aed,
    COUNT(*)                                                                AS sample_size,
    ROUND(100.0 * COUNT(*) FILTER (WHERE salary_source = 'reported') / COUNT(*), 0)
                                                                            AS pct_reported
FROM jobs
WHERE salary_aed_monthly BETWEEN 2000 AND 200000
GROUP BY title_normalized, emirate, sector, seniority_band, salary_source   -- FIX: salary_source in GROUP BY
HAVING COUNT(*) >= 2
ORDER BY avg_salary_aed DESC;


-- ── vw_skill_salary_premium ───────────────────────────────────────────────────
-- Powers: Scatter — Skill Salary Premium
-- Each skill shows its avg salary vs the overall average (premium = % above baseline)

CREATE OR REPLACE VIEW vw_skill_salary_premium AS
WITH baseline AS (
    SELECT AVG(salary_aed_monthly) AS overall_avg
    FROM jobs WHERE salary_aed_monthly BETWEEN 2000 AND 200000
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
    ROUND((ss.avg_salary_aed - b.overall_avg) / b.overall_avg * 100, 1) AS salary_premium_pct
FROM skill_salaries ss
CROSS JOIN baseline b
ORDER BY salary_premium_pct DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- PAGE 3: Skills Intelligence
-- ════════════════════════════════════════════════════════════════════════════

-- ── vw_skill_heatmap ─────────────────────────────────────────────────────────
-- Powers: Matrix visual — Skill × Emirate × Sector heatmap
-- FIX: Added job_id to SELECT and GROUP BY so the Power BI model relationship
--      vw_jobs_detail[job_id] → vw_skill_heatmap[job_id] (Part 2) resolves
--      correctly. Without job_id the relationship key is missing entirely.

CREATE OR REPLACE VIEW vw_skill_heatmap AS
SELECT
    job_id,                                                         -- FIX: required for Power BI relationship
    skill,
    emirate,
    sector,
    seniority_band,
    COUNT(*)                                                        AS count,
    ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed
FROM job_skills
WHERE skill IS NOT NULL AND skill != ''
GROUP BY job_id, skill, emirate, sector, seniority_band;            -- FIX: job_id added to GROUP BY


-- ── vw_skill_cooccurrence ────────────────────────────────────────────────────
-- Powers: Skills network / co-occurrence matrix
-- Shows which pairs of skills appear together most often

CREATE OR REPLACE VIEW vw_skill_cooccurrence AS
SELECT
    a.skill AS skill_a,
    b.skill AS skill_b,
    COUNT(*) AS cooccurrence_count,
    COUNT(DISTINCT a.emirate) AS emirate_spread
FROM job_skills a
JOIN job_skills b
    ON a.job_id = b.job_id
    AND a.skill < b.skill    -- prevent duplicate pairs
WHERE a.skill IS NOT NULL AND b.skill IS NOT NULL
GROUP BY a.skill, b.skill
HAVING COUNT(*) >= 5
ORDER BY cooccurrence_count DESC
LIMIT 200;


-- ── vw_emerging_skills ────────────────────────────────────────────────────────
-- Powers: "Trending Skills" card/table — skills growing fastest in last 30 days

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
    WHERE posted_at BETWEEN NOW() - INTERVAL '60 days' AND NOW() - INTERVAL '30 days'
    GROUP BY skill
)
SELECT
    c.skill,
    c.current_count,
    COALESCE(p.prior_count, 0)          AS prior_count,
    c.current_count - COALESCE(p.prior_count, 0) AS absolute_growth,
    CASE
        WHEN COALESCE(p.prior_count, 0) = 0
        THEN NULL   -- new skill, can't compute % growth
        ELSE ROUND(100.0 * (c.current_count - p.prior_count) / p.prior_count, 1)
    END AS growth_pct
FROM current_period c
LEFT JOIN prior_period p USING (skill)
WHERE c.current_count >= 5
ORDER BY absolute_growth DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- PAGE 4: Company Intelligence
-- ════════════════════════════════════════════════════════════════════════════

-- ── vw_company_radar ─────────────────────────────────────────────────────────
-- Powers: Scatter plot — Company Radar
-- X: avg_salary_aed | Y: open_roles | Size: active_weeks | Colour: sector

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
    -- Hiring intensity: roles per active week
    ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT DATE_TRUNC('week', j.posted_at)), 0), 1)
                                                                    AS hiring_intensity
FROM jobs j
WHERE j.company IS NOT NULL AND j.company != ''
GROUP BY j.company, j.emirate, j.sector,
         j.company_size_band, j.is_mnc, j.company_hq_country
ORDER BY open_roles DESC
LIMIT 100;


-- ── vw_mnc_vs_local ──────────────────────────────────────────────────────────
-- Powers: Grouped bar — MNC vs Local salary and demand comparison

CREATE OR REPLACE VIEW vw_mnc_vs_local AS
SELECT
    CASE WHEN is_mnc THEN 'MNC' ELSE 'Local / Regional' END        AS company_type,
    sector,
    emirate,
    COUNT(*)                                                        AS job_count,
    ROUND(AVG(salary_aed_monthly) FILTER (WHERE salary_aed_monthly > 0)) AS avg_salary_aed,
    ROUND(AVG(skills_count), 1)                                     AS avg_skills_required
FROM jobs
WHERE is_mnc IS NOT NULL
GROUP BY 1, 2, 3;


-- ════════════════════════════════════════════════════════════════════════════
-- PAGE 5: Data Quality & Pipeline Health
-- ════════════════════════════════════════════════════════════════════════════

-- ── vw_pipeline_health ───────────────────────────────────────────────────────
-- Powers: KPI cards + data quality gauges

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
    ROUND(AVG(CURRENT_DATE - posted_at::date), 0)                          AS avg_days_since_posted,
    COUNT(DISTINCT company)                                                AS unique_companies,
    COUNT(DISTINCT emirate)                                                AS unique_emirates
FROM jobs;


-- ── vw_field_coverage ────────────────────────────────────────────────────────
-- Powers: Field coverage matrix — which fields are populated by what %

CREATE OR REPLACE VIEW vw_field_coverage AS
SELECT 'title'               AS field, ROUND(100.0 * COUNT(*) FILTER (WHERE title IS NOT NULL AND title != '') / COUNT(*), 1)             AS pct_filled FROM jobs UNION ALL
SELECT 'company',                      ROUND(100.0 * COUNT(*) FILTER (WHERE company IS NOT NULL AND company != '') / COUNT(*), 1)          FROM jobs UNION ALL
SELECT 'emirate',                      ROUND(100.0 * COUNT(*) FILTER (WHERE emirate IS NOT NULL) / COUNT(*), 1)                            FROM jobs UNION ALL
SELECT 'sector',                       ROUND(100.0 * COUNT(*) FILTER (WHERE sector != 'Other') / COUNT(*), 1)                              FROM jobs UNION ALL
SELECT 'salary_aed_monthly',           ROUND(100.0 * COUNT(*) FILTER (WHERE salary_aed_monthly > 0) / COUNT(*), 1)                         FROM jobs UNION ALL
SELECT 'salary_reported',              ROUND(100.0 * COUNT(*) FILTER (WHERE salary_source = 'reported') / COUNT(*), 1)                     FROM jobs UNION ALL
SELECT 'seniority_band',               ROUND(100.0 * COUNT(*) FILTER (WHERE seniority_band != 'Mid-Level') / COUNT(*), 1)                  FROM jobs UNION ALL
SELECT 'company_size_band',            ROUND(100.0 * COUNT(*) FILTER (WHERE company_size_band IS NOT NULL AND company_size_band != 'Unknown') / COUNT(*), 1) FROM jobs UNION ALL
SELECT 'posted_at',                    ROUND(100.0 * COUNT(*) FILTER (WHERE posted_at IS NOT NULL) / COUNT(*), 1)                          FROM jobs UNION ALL
SELECT 'skills_count >= 1',            ROUND(100.0 * COUNT(*) FILTER (WHERE skills_count >= 1) / COUNT(*), 1)                              FROM jobs;


-- ── vw_pipeline_runs ─────────────────────────────────────────────────────────
-- Powers: Pipeline run log table — shows each Monday refresh

CREATE OR REPLACE VIEW vw_pipeline_runs AS
SELECT
    id,
    run_at,
    db_target,
    jobs_total,
    jobs_inserted,
    skill_tags,
    ROUND(salary_coverage, 1)  AS salary_coverage_pct,
    ROUND(avg_completeness, 1) AS avg_completeness_score,
    status,
    notes
FROM pipeline_runs
ORDER BY run_at DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- PAGE 6: Job Explorer (drill-through)
-- ════════════════════════════════════════════════════════════════════════════

-- ── vw_jobs_detail ────────────────────────────────────────────────────────────
-- Powers: Full job listing table with drill-through from any page
-- FIX: Added days_since_posted so the Job Explorer table can expose it
--      (vw_pipeline_health computes avg_days_since_posted from the raw jobs
--      table; this makes the per-row value available in Power BI as well).

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
    (CURRENT_DATE - posted_at::date)                               AS days_since_posted,  -- FIX: added
    apply_link
FROM jobs
ORDER BY posted_at DESC NULLS LAST;