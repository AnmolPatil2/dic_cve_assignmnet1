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
├── Screenshots/                      # Execution proofs (PDFs) and visualizations (PNGs)
│   ├── 01_Bronze_Ingestion_proof.pdf
│   ├── 02_Silver_Normalization_proof.pdf
│   ├── 03_Exploratory_Analysis_ricks.pdf
│   ├── 3rd_analysis.png              # Monthly publication patterns
│   ├── 4th_analysis.png              # CVSS risk distribution (pie chart)
│   ├── 5th_analysis.png              # Top 25 vendors (bar chart)
│   ├── 6th_analysis.png              # CVE state distribution
│   ├── 7th_analysis.png              # Market concentration analysis
│   └── 8th_analysis.png              # Monthly trends with CVSS scores
│
└── ipynb_for_clear_understanding/   # Jupyter notebooks for reference
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
- Filters to 2024 CVEs by publication date
- Data quality checks (count, nulls, uniqueness)
- Creates `cve_bronze.records` table

**Output:** 32,924 CVE records

**Run:** Click "Run All"

**Verify:**
```sql
SELECT COUNT(*) FROM cve_bronze.records;  -- Returns: 32,924
```

---

#### 🥈 Silver Layer (`02_Silver_Normalization.py`)

**Runtime:** ~1-2 minutes

**What it does:**
- Normalizes CVE data from nested JSON
- Extracts core fields: CVE ID, dates, CVSS scores, descriptions
- Handles multiple CVSS versions (v3.1, v3.0, v2.0) with coalesce
- **Explodes** vendor/product arrays into separate rows
- Creates relational one-to-many tables

**Output:**
- `cve_silver.core` - One row per CVE
- `cve_silver.affected_products` - One row per vendor/product combination

**Run:** Click "Run All"

**Verify:**
```sql
SELECT COUNT(*) FROM cve_silver.core;              -- Returns: 32,924
SELECT COUNT(*) FROM cve_silver.affected_products; -- Returns: 50,000+
```

---

#### 🥇 Gold Layer (`03_Gold_Analysis.py` or `.sql`)

**Runtime:** <1 minute per query

**What it does:**
- Performs 9 security intelligence analyses
- Generates interactive visualizations
- Provides temporal trends, risk assessment, and vendor intelligence

**Analyses:**
1. Yearly CVE publication trends
2. Publication latency (reserved → published)
3. Monthly publication patterns (seasonality)
4. CVSS risk distribution by severity
5. Top 25 vendors by vulnerability count
6. CVE state distribution (Published vs Rejected)
7. Market concentration (Top 10/25 vendors)
8. Monthly trends with average CVSS scores
9. Seasonal vulnerability patterns

**Run:** Execute cells individually, click chart icons for visualizations

---

## Key Results & Insights

### Data Processing Metrics

| Metric | Value | Description |
|--------|-------|-------------|
| **Total CVE Records (2024)** | 32,924 | Record-breaking year for vulnerabilities |
| **Unique Vendors Affected** | 1,500+ | Organizations with reported vulnerabilities |
| **Vendor/Product Combinations** | 50,000+ | Result of exploding nested arrays |
| **CVEs with CVSS Scores** | 85%+ | Vulnerabilities with severity ratings |
| **Avg Products per CVE** | ~1.5 | One vulnerability affects multiple products |

### Security Intelligence Insights

**Risk Distribution:**
- **CRITICAL** (9.0-10.0): High-impact vulnerabilities requiring immediate attention
- **HIGH** (7.0-8.9): Serious vulnerabilities needing prompt patching
- **MEDIUM** (4.0-6.9): Moderate risk vulnerabilities
- **LOW** (0.1-3.9): Lower priority but trackable issues

**Vendor Intelligence:**
- Top vendors by vulnerability count identified
- Market concentration shows if vulnerabilities are concentrated in few vendors or distributed
- Helps prioritize security efforts based on vendor exposure

**Temporal Patterns:**
- Monthly publication patterns reveal disclosure trends
- Seasonal analysis shows if certain times of year have more disclosures
- Publication latency metrics indicate time from discovery to disclosure

---

## Documentation

### Execution Proof PDFs (in Screenshots/ folder)

Complete notebook outputs showing all code, results, and visualizations:

- **`01_Bronze_Ingestion_proof.pdf`**  
  Shows data ingestion, filtering, quality checks, and Bronze table creation

- **`02_Silver_Normalization_proof.pdf`**  
  Shows JSON parsing, explode operations, table creation, and data verification

- **`03_Exploratory_Analysis_ricks.pdf`**  
  Shows all 9 analytical queries with results, charts, and insights

### Analysis Visualizations (in Screenshots/ folder)

6 PNG images showing key analysis results:
- Monthly publication patterns (line chart)
- CVSS risk distribution (pie chart)
- Top 25 vendors by vulnerability count (bar chart)
- CVE state distribution (Published vs Rejected)
- Market concentration analysis
- Monthly trends with average CVSS scores

---

## Architecture

**Bronze Layer (Raw Ingestion)**
- Input: CVE v5 JSON in Parquet format
- Output: `cve_bronze.records` Delta table
- Purpose: Immutable source of truth

**Silver Layer (Normalization)**
- Input: `cve_bronze.records`
- Output: `cve_silver.core` + `cve_silver.affected_products`
- Purpose: Cleaned, normalized relational tables

**Gold Layer (Analytics)**
- Input: Silver layer tables
- Output: Business intelligence queries and visualizations
- Purpose: Security insights and trend analysis

---

## Technologies

- **Platform:** Databricks Community Edition (DBR 13.x+)
- **Processing:** Apache Spark / PySpark
- **Storage:** Delta Lake (ACID transactions)
- **Languages:** Python & SQL
- **Architecture:** Medallion (Bronze → Silver → Gold)

---

## Data Quality Validation

**Bronze Layer:**
✅ 32,924 records (exceeds 30,000 minimum)  
✅ Zero null CVE IDs  
✅ All CVE IDs unique  
✅ All records from 2024  

**Silver Layer:**
✅ One row per CVE in core table  
✅ CVSS scores properly extracted  
✅ Vendor/product arrays successfully exploded  
✅ Foreign key relationships maintained  

---

## Troubleshooting

**"Table not found"**  
→ Run notebooks in order: 01 → 02 → 03

**"Path not found"**  
→ Verify parquet file uploaded to `/Volumes/workspace/default/cve_lakehouse_data/2024_parquet.parquet`

**"Volume does not exist"**  
→ Create volume: `CREATE VOLUME workspace.default.cve_lakehouse_data;`

**"Out of memory"**  
→ Restart cluster (Databricks Community Edition has limited resources)

---

## Repository

**URL:** https://github.com/AnmolPatil2/dic_cve_assignmnet1  
**Course:** DIC 587 - Data Intensive Computing  
**Due Date:** November 16, 2025

---

**Data Source:** [CVEProject/cvelistV5](https://github.com/CVEProject/cvelistV5)  
**Platform:** Databricks Community Edition
