# 🇦🇪 UAE Talent Intelligence Dashboard

> A live analytics platform tracking skill demand, salary benchmarks,
> and hiring velocity across all seven UAE emirates — refreshed automatically every Monday.

---

## 📌 Business Problem

UAE HR and recruitment teams lack real-time visibility into how the job market is shifting week to week.
This dashboard ingests live job postings from LinkedIn, Indeed, and Glassdoor (via JSearch + Adzuna APIs)
and surfaces actionable intelligence for hiring managers, HR business partners, and TA teams.

**Questions this dashboard answers:**
- Which skills command the highest salary premiums in Dubai vs Abu Dhabi?
- Which companies are growing headcount the fastest?
- How has hiring velocity changed over the last 90 days?
- What is the remote work penetration by sector and emirate?

---

## 🖥 Live Dashboard

> 📊 [View Power BI Report](#)  
> ▶ [90-second walkthrough (Loom)](#)

---

## 🏗 Architecture

```
JSearch API  ──┐
Adzuna API   ──┤──► Python ETL ──► PostgreSQL ──► Power BI
Exchange API ──┘        │
                   GitHub Actions
                (auto-runs every Monday)
```

### Pipeline steps

| Step | File | What it does |
|------|------|-------------|
| Ingest | `etl/fetch_jobs.py` | Calls JSearch + Adzuna APIs, saves raw JSON |
| Transform | `etl/transform.py` | Cleans data, extracts skills, normalises salaries to AED |
| Load | `etl/load_db.py` | Upserts to PostgreSQL, creates KPI views |

---

## 🛠 Tech Stack

| Layer | Tools |
|-------|-------|
| Data collection | Python `requests`, JSearch API, Adzuna API |
| Transformation | `pandas`, `spaCy` |
| Storage | PostgreSQL |
| Visualisation | Power BI Desktop |
| Automation | GitHub Actions (CRON — every Monday) |
| Environment | `python-dotenv` |

---

## 🚀 Running Locally

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/uae-talent-intelligence.git
cd uae-talent-intelligence
```

### 2. Set up environment

```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### 3. Configure `.env`

```bash
cp .env.example .env
# Edit .env and fill in your API keys
```

### 4. Create the database

```bash
psql -U postgres -c "CREATE DATABASE uae_talent;"
psql -U postgres -d uae_talent -f sql/schema.sql
```

### 5. Run the pipeline

```bash
python run_pipeline.py
```

---

## 📊 Dashboard Pages

| Page | Visual | Insight |
|------|--------|---------|
| Skill Demand | Ranked bar chart | Top 30 in-demand skills across UAE |
| Salary Intelligence | Range bars + scatter | Salary bands by role, emirate, sector |
| Hiring Pulse | Line chart | Weekly jobs posted trend |
| Skill Heatmap | Matrix | Skill × emirate demand concentration |
| Company Radar | Scatter plot | Open roles vs avg salary per company |
| Job Detail | Table + drill-through | Individual postings with apply link |

---

## 📁 Project Structure

```
uae-talent-intelligence/
├── .env.example
├── .gitignore
├── requirements.txt
├── run_pipeline.py          ← run the full pipeline
├── etl/
│   ├── fetch_jobs.py        ← API ingestion
│   ├── transform.py         ← cleaning + skill extraction
│   └── load_db.py           ← PostgreSQL loader + views
├── sql/
│   ├── schema.sql           ← table + index definitions
│   └── kpi_views.sql        ← all Power BI views
├── dashboard/
│   └── UAE_Talent_Intel.pbix
├── .github/
│   └── workflows/
│       └── weekly_refresh.yml
└── README.md
```

---

## 🔑 API Sources

| Source | Link | Use |
|--------|------|-----|
| JSearch (RapidAPI) | https://rapidapi.com/letscrape-6bRWetWOEM/api/jsearch | LinkedIn + Indeed jobs |
| Adzuna | https://developer.adzuna.com | Official UAE job API |
| ExchangeRate-API | https://www.exchangerate-api.com | AED salary normalisation |

---

## 👤 Author

Built by [Your Name] · [LinkedIn](#) · [Portfolio](#)