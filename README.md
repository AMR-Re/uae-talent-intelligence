# 🇦🇪 UAE Talent Intelligence Platform

> A production-grade workforce analytics platform tracking skill demand,
> salary benchmarks, and hiring velocity across all seven UAE emirates —
> refreshed automatically every Monday via GitHub Actions.

---

## 📌 Business Problem

UAE HR and recruitment teams lack real-time visibility into how the job market
shifts week to week. This platform ingests live job postings from LinkedIn,
Indeed, and Glassdoor (via JSearch + Adzuna APIs) and surfaces intelligence for:

- **HR Business Partners** — "Which skills should we require in our next hire?"
- **Talent Acquisition** — "What salary do I need to offer to close this role in Dubai?"
- **Compensation & Benefits** — "How do our pay bands compare to the market?"
- **People Analytics** — "Which companies are growing headcount fastest in our sector?"

---

## 🖥 Live Dashboard

> 📊 [View Power BI Report](#) — replace with your published URL
> ▶ [90-second walkthrough](#) — replace with your Loom link

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
│  JSearch API ──┐                                            │
│  Adzuna API  ──┼──► data/raw/  (JSON)                       │
│  ExchangeAPI ──┘                                            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                  ETL PIPELINE (Python)                       │
│                                                             │
│  fetch_jobs.py  → raw JSON                                  │
│       ↓                                                     │
│  transform.py   → clean · detect emirate · classify sector  │
│                   extract skills · derive seniority         │
│                   enrich companies · impute salaries (Tier1)│
│       ↓                                                     │
│  salary_model.py → GBR imputation (Tier 2)                  │
│       ↓                                                     │
│  load_db.py     → upsert PostgreSQL · create KPI views      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    DATABASE (PostgreSQL)                     │
│  Tables:  jobs · job_skills · pipeline_runs                 │
│  Views:   8 KPI views → Power BI                            │
│  Hosts:   local (dev) + Supabase (prod/cloud)               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    POWER BI REPORT                           │
│  Page 1: Market Overview                                    │
│  Page 2: Salary Intelligence                                │
│  Page 3: Skills Intelligence                                │
│  Page 4: Company Intelligence                               │
│  Page 5: Job Explorer (drill-through)                       │
│  Page 6: Data Quality & Pipeline Health                     │
└─────────────────────────────────────────────────────────────┘
                     ↑
              GitHub Actions
        (auto-refresh every Monday)
```

---

## 💡 What Makes This Different

Most job market dashboards just count postings. This platform goes further:

| Feature | Detail |
|---------|--------|
| **Three-tier salary imputation** | Reported → peer-median → GBR model. Every salary has a `salary_source` flag so consumers know confidence level |
| **Company enrichment layer** | Adds MNC flag, company size band, HQ country, Glassdoor rating |
| **Seniority extraction** | 6-band classification from job titles (C-Suite → Junior) |
| **Completeness scoring** | Every record gets a 0–100 quality score — surfaced in Power BI |
| **Skills co-occurrence** | Shows which skill pairs appear together (network analysis) |
| **Skill salary premium** | Computes each skill's % salary uplift vs the baseline |
| **Pipeline health page** | A dedicated dashboard page showing data quality metrics — unusual for portfolio projects and impressive in interviews |

---

## 🛠 Tech Stack

| Layer | Tool |
|-------|------|
| Data collection | Python `requests`, JSearch (RapidAPI), Adzuna |
| Company enrichment | OpenCorporates API, `rapidfuzz` fuzzy matching |
| Transformation | `pandas`, `numpy`, regex taxonomy |
| Salary imputation | `scikit-learn` GradientBoostingRegressor |
| Storage | PostgreSQL (local) + Supabase (cloud) |
| ORM / queries | SQLAlchemy 2.0 |
| Visualisation | Power BI Desktop + Power BI Service |
| Automation | GitHub Actions (CRON — every Monday 06:00 UAE time) |
| Environment | `python-dotenv`, `colorama`, `tenacity`, `tqdm` |

---

## 🚀 Setup Guide

### 1. Prerequisites

- Python 3.11+
- PostgreSQL 15+ (local)
- Power BI Desktop (free — Windows only for Desktop; use Power BI Service for Mac)
- API keys — see below

### 2. Clone the repository

```bash
git clone https://github.com/AMR-Re/uae-talent-intelligence.git
cd uae-talent-intelligence
```

### 3. Python environment

```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

### 4. API keys you need (all have free tiers)

| API | Where to sign up | Free tier |
|-----|-----------------|-----------|
| JSearch (RapidAPI) | https://rapidapi.com/letscrape-6bRWetWOEM/api/jsearch | 200 req/month |
| Adzuna | https://developer.adzuna.com | Unlimited (rate limited) |
| ExchangeRate-API | https://www.exchangerate-api.com | 1,500 req/month |
| OpenCorporates | https://opencorporates.com/api_accounts/new | 50 req/day (optional) |

### 5. Configure environment

```bash
cp .env.example .env
# Edit .env and fill in your API keys and DB credentials
```

### 6. Create the database

```bash
# Create database
psql -U postgres -c "CREATE DATABASE uae_talent;"

# Run schema (optional — pipeline does this automatically)
psql -U postgres -d uae_talent -f sql/schema.sql
```

### 7. Run the full pipeline

```bash
# Full run (fetch → transform → ML → load)
python run_pipeline.py

# Skip API calls (use existing raw data)
python run_pipeline.py --skip-fetch

# Transform + ML only (no DB load)
python run_pipeline.py --transform-only

# Skip ML step (faster, less accurate salaries)
python run_pipeline.py --skip-ml
```

### 8. Sync to Supabase (cloud)

```bash
# After loading locally, push to cloud:
python sync_db.py
```

---

## 📊 Power BI Setup

Connect Power BI to your PostgreSQL database:

1. Open Power BI Desktop → **Get Data → PostgreSQL**
2. Server: `localhost:5432` (or your Supabase host)
3. Database: `uae_talent`
4. Import these tables/views:

```
vw_top_skills          → Page 1 (Market Overview)
vw_hiring_velocity     → Page 1 (Market Overview)
vw_remote_analysis     → Page 1 (Market Overview)
vw_salary_bands        → Page 2 (Salary Intelligence)
vw_skill_salary_premium → Page 2 (Salary Intelligence)
vw_skill_heatmap       → Page 3 (Skills Intelligence)
vw_skill_cooccurrence  → Page 3 (Skills Intelligence)
vw_emerging_skills     → Page 3 (Skills Intelligence)
vw_company_radar       → Page 4 (Company Intelligence)
vw_mnc_vs_local        → Page 4 (Company Intelligence)
vw_jobs_detail         → Page 5 (Job Explorer)
vw_pipeline_health     → Page 6 (Data Quality)
vw_field_coverage      → Page 6 (Data Quality)
vw_pipeline_runs       → Page 6 (Data Quality)
```

See `dashboard/POWERBI_GUIDE.md` for detailed visual configuration.

---

## 📁 Project Structure

```
uae-talent-intelligence/
├── .env.example                 ← copy to .env and fill in
├── .gitignore
├── requirements.txt
├── run_pipeline.py              ← master pipeline runner
├── sync_db.py                   ← push local → Supabase
│
├── etl/
│   ├── __init__.py
│   ├── db.py                    ← database connection manager
│   ├── fetch_jobs.py            ← JSearch + Adzuna API ingestion
│   ├── enrich_companies.py      ← company enrichment (OC API + heuristics)
│   ├── transform.py             ← clean · extract · impute (Tier 1)
│   ├── salary_model.py          ← GBR salary imputation (Tier 2)
│   
├── extraction/
│    └── load_db.py               ← PostgreSQL upsert + view creation      
├── sql/
│   ├── schema.sql               ← table + index definitions
│   └── kpi_views.sql            ← all 8 Power BI views with comments
│
├── data/
│   ├── raw/                     ← gitignored — API JSON responses
│   ├── processed/               ← gitignored — clean CSVs
│   └── reference/
│       ├── uae_salary_benchmarks.csv  ← hand-curated salary benchmarks
│       ├── company_profiles.csv       ← hand-curated company data
│       └── company_cache.json         ← gitignored — API cache
│
├── dashboard/
│   ├── UAE_Talent_Intel.pbix    ← Power BI report file
│   └── POWERBI_GUIDE.md         ← detailed visual build guide
│
└── .github/
    └── workflows/
        └── weekly_refresh.yml   ← Monday 06:00 UAE auto-refresh
```

---

## 🔑 Data Dictionary

### `jobs` table

| Column | Type | Description |
|--------|------|-------------|
| `job_id` | TEXT | Primary key (from API) |
| `title_normalized` | TEXT | Standardised title bucket (e.g. "HR Business Partner") |
| `emirate` | TEXT | Dubai / Abu Dhabi / Sharjah / ... |
| `seniority_band` | TEXT | C-Suite / Director+ / Manager / Senior / Mid-Level / Junior |
| `salary_aed_monthly` | NUMERIC | Monthly salary in AED (normalised midpoint) |
| `salary_source` | TEXT | How salary was obtained: `reported` / `imputed_peer_L1..L4` / `imputed_model` / `imputed_benchmark` |
| `company_size_band` | TEXT | 1-50 / 51-200 / 201-1000 / 1000+ |
| `is_mnc` | BOOLEAN | True = multinational corporation |
| `completeness_score` | INTEGER | 0–100 data quality score |

### `job_skills` table (long/exploded form)

One row per skill per job. Join back to `jobs` on `job_id`.

---

## 👤 Author

Built by [Amr Rezk] · [LinkedIn](https://www.linkedin.com/in/amr-re/) 

---

## 📜 Licence

Data from public APIs. No individual personal data is stored.