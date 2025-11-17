# CVE Lakehouse on Databricks - Medallion Architecture

## Project Overview

A cybersecurity data lakehouse analyzing **32,924 CVE records from 2024** using Databricks and Delta Lake. Implements the Medallion Architecture (Bronze → Silver → Gold) for vulnerability intelligence.

**Course:** DIC 587 - Data Intensive Computing | Fall 2025

---

## Project Structure
```
dic_cve_assignmnet1/
│
├── README.md
│
├── data/
│   └── 2024_parquet.parquet          # CVE 2024 source data
│
├── source_code_as_per_submission/
│   ├── 01_Bronze_Layer.py           
│   ├── 02_Silver_Normalization.py   
│   ├── 03_Gold_Analysis.py          
│   └── 03_Gold_Analysis.sql         
│
├── Screenshots/
│   ├── 3rd_analysis.png             
│   ├── 4th_analysis.png             
│   ├── 5th_analysis.png             
│   ├── 6th_analysis.png             
│   ├── 7th_analysis.png             
│   └── 8th_analysis.png             
│
└── ipynb_for_clear_understanding/
    ├── 01_Bronze_Ingestion_proof.pdf
    ├── 02_Silver_Normalization_proof.pdf
    └── 03_Exploratory_Analysis_ricks.pdf
```

---

## How to Run

### Step 1: Setup Databricks

1. Create account at https://community.cloud.databricks.com/
2. Create volume:
```sql
   CREATE VOLUME workspace.default.cve_lakehouse_data;
```
3. Upload `data/2024_parquet.parquet` to:  
   `/Volumes/workspace/default/cve_lakehouse_data/2024_parquet.parquet`

---

### Step 2: Import Notebooks

1. In Databricks, go to **Workspace**
2. Right-click → **Import**
3. Import all files from `source_code_as_per_submission/`

---

### Step 3: Execute Pipeline

#### 🥉 Bronze Layer (`01_Bronze_Layer.py`)

**Runtime:** ~2-3 minutes

**What it does:**
- Reads Parquet file
- Filters to 2024 CVEs
- Creates `cve_bronze.records` table (32,924 records)

**Run:** Click "Run All"

**Verify:**
```sql
SELECT COUNT(*) FROM cve_bronze.records;  -- Should return 32,924
```

---

#### 🥈 Silver Layer (`02_Silver_Normalization.py`)

**Runtime:** ~1-2 minutes

**What it does:**
- Normalizes CVE data
- Extracts core fields (dates, CVSS, descriptions)
- Explodes vendor/product arrays
- Creates 2 tables:
  - `cve_silver.core` (32,924 records)
  - `cve_silver.affected_products` (50,000+ records)

**Run:** Click "Run All"

**Verify:**
```sql
SELECT COUNT(*) FROM cve_silver.core;              -- 32,924
SELECT COUNT(*) FROM cve_silver.affected_products; -- 50,000+
```

---

#### 🥇 Gold Layer (`03_Gold_Analysis.py` or `.sql`)

**Runtime:** <1 minute per query

**What it does:**
- 9 analytical queries:
  1. Yearly CVE trends
  2. Publication latency
  3. Monthly patterns
  4. CVSS risk distribution
  5. Top 25 vendors
  6. CVE state distribution
  7. Market concentration
  8. Monthly trends with CVSS
  9. Seasonal patterns

**Run:** Execute cells individually, use chart icons for visualizations

---

## Architecture

- **Bronze:** Raw data ingestion (JSON → Delta)
- **Silver:** Normalized tables (core CVE + exploded products)
- **Gold:** Business analytics (9 security intelligence queries)

---

## Key Results

| Metric | Value |
|--------|-------|
| CVE Records (2024) | 32,924 |
| Unique Vendors | 1,500+ |
| Vendor/Product Combos | 50,000+ |
| CVEs with CVSS Scores | 85%+ |

---

## Technologies

- Databricks Community Edition (DBR 13.x+)
- Apache Spark / PySpark
- Delta Lake
- Python & SQL

---

## Troubleshooting

**"Table not found"**  
→ Run notebooks in order: 01 → 02 → 03

**"Path not found"**  
→ Verify parquet file uploaded to correct volume path

**"Volume does not exist"**  
→ Create volume first (see Step 1)

---

## Repository

**URL:** https://github.com/AnmolPatil2/dic_cve_assignmnet1  
**Course:** DIC 587 - Data Intensive Computing  
**Due Date:** November 16, 2025

---

**Data Source:** [CVEProject/cvelistV5](https://github.com/CVEProject/cvelistV5)
