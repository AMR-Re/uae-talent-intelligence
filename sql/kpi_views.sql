-- kpi_views.sql
-- Power BI connects directly to these views.
-- run_load.py creates these automatically,
-- but you can also run this manually.

-- Top 30 skills by demand
CREATE OR REPLACE VIEW vw_top_skills AS
SELECT
    skill,
    COUNT(*)                    AS demand_count,
    COUNT(DISTINCT emirate)     AS emirate_spread,
    COUNT(DISTINCT sector)      AS sector_spread
FROM job_skills
WHERE skill IS NOT NULL AND skill != ''
GROUP BY skill
ORDER BY demand_count DESC
LIMIT 30;


-- Salary intelligence by role and emirate
CREATE OR REPLACE VIEW vw_salary_bands AS
SELECT
    title,
    emirate,
    sector,
    ROUND(AVG(salary_aed_monthly))                                          AS avg_salary_aed,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS p25_aed,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_aed_monthly)) AS p75_aed,
    COUNT(*)                                                                AS sample_size
FROM jobs
WHERE salary_aed_monthly BETWEEN 1000 AND 200000
GROUP BY title, emirate, sector
HAVING COUNT(*) >= 2;


-- Weekly hiring velocity by emirate and sector
CREATE OR REPLACE VIEW vw_hiring_velocity AS
SELECT
    DATE_TRUNC('week', posted_at)  AS week_start,
    emirate,
    sector,
    COUNT(*)                       AS jobs_posted
FROM jobs
WHERE posted_at IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC;


-- Skill × emirate heatmap (Power BI matrix visual)
CREATE OR REPLACE VIEW vw_skill_heatmap AS
SELECT
    skill,
    emirate,
    sector,
    COUNT(*) AS count
FROM job_skills
WHERE emirate NOT IN ('Other UAE', 'Unknown')
  AND skill IS NOT NULL
GROUP BY skill, emirate, sector;


-- Company hiring radar (scatter: open roles vs avg salary)
CREATE OR REPLACE VIEW vw_company_radar AS
SELECT
    company,
    emirate,
    sector,
    COUNT(*)                                          AS open_roles,
    ROUND(AVG(salary_aed_monthly))                    AS avg_salary_aed,
    COUNT(DISTINCT DATE_TRUNC('week', posted_at))     AS active_weeks
FROM jobs
WHERE company IS NOT NULL AND company != ''
GROUP BY company, emirate, sector
ORDER BY open_roles DESC
LIMIT 50;


-- Remote vs on-site breakdown
CREATE OR REPLACE VIEW vw_remote_analysis AS
SELECT
    sector,
    emirate,
    remote,
    COUNT(*)                        AS job_count,
    ROUND(AVG(salary_aed_monthly))  AS avg_salary_aed
FROM jobs
GROUP BY sector, emirate, remote;


-- Full job listing (for drill-through page in Power BI)
CREATE OR REPLACE VIEW vw_jobs_detail AS
SELECT
    job_id, source, title, company, emirate, sector,
    employment_type, remote,
    salary_aed_monthly,
    skills_count,
    posted_at,
    apply_link
FROM jobs
ORDER BY posted_at DESC;