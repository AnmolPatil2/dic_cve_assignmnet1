# CVE Lakehouse on Databricks - Medallion Architecture

## Project Overview

A cybersecurity data lakehouse analyzing **32,924 CVE records from 2024** using Databricks and Delta Lake. Implements the Medallion Architecture (Bronze → Silver → Gold) for vulnerability intelligence.

**Course:** DIC 587 - Data Intensive Computing  
**Assignment:** CVE Lakehouse Implementation  
**Fall 2025**

---

## Project Structure
```
cve-lakehouse-project/
│
├── README.md
│
├── source_code_as_per_submission/
│   ├── 01_Bronze_Layer.py           # Bronze layer ingestion (~2-3 min runtime)
│   ├── 02_Silver_Normalization.py   # Silver layer transformations (~1-2 min runtime)
│   ├── 03_Gold_Analysis.py          # Gold layer analytics - Python version
│   └── 03_Gold_Analysis.sql         # Gold layer analytics - SQL version
│
├── Screenshots/
│   ├── 3rd_analysis.png             # Monthly publication patterns
│   ├── 4th_analysis.png             # CVSS risk distribution
│   ├── 5th_analysis.png             # Top 25 vendors by vulnerability count
│   ├── 6th_analysis.png             # CVE state distribution
│   ├── 7th_analysis.png             # Market concentration analysis
│   └── 8th_analysis.png             # Monthly trends with CVSS scores
│
└── ipynb_for_clear_understanding/
    ├── 01_Bronze_Ingestion_proof.pdf
    ├── 02_Silver_Normalization_proof.pdf
    └── 03_Exploratory_Analysis_ricks.pdf
```

---

## Quick Setup Instructions

### Step 1: Databricks Environment Setup (5 minutes)

1. **Create Databricks Community Edition Account:**
   - Go to https://community.cloud.databricks.com/
   - Sign up with Google account (2FA required)

2. **Create Volume:**
```sql
   CREATE VOLUME workspace.default.cve_lakehouse_data;
```

3. **Upload CVE Data:**
   - Upload `2024_parquet.parquet` to `/Volumes/workspace/default/cve_lakehouse_data/`

### Step 2: Run the Pipeline

#### Bronze Layer (Runtime: ~2-3 minutes)

**File:** `01_Bronze_Layer.py`

**What it does:**
- Reads 2024 CVE data from Parquet
- Filters to 2024 records by publication date
- Performs data quality checks (count, nulls, uniqueness)
- Writes to Delta table: `cve_bronze.records`

**Output:**
- 32,924 CVE records in Bronze layer
- Table: `cve_bronze.records`

**Run:**
```python
# Import notebook or copy code cells sequentially
# All configuration is in Cell 1
```

---

#### Silver Layer (Runtime: ~1-2 minutes)

**File:** `02_Silver_Normalization.py`

**What it does:**
- Extracts core CVE fields (ID, dates, CVSS scores, descriptions)
- Handles multiple CVSS versions (v3.1, v3.0, v2.0)
- **Explodes** nested vendor/product arrays into relational tables
- Creates normalized tables

**Output:**
- `cve_silver.core` - 32,924 normalized CVE records
- `cve_silver.affected_products` - 50,000+ vendor/product combinations

**Run:**
```python
# Requires: Bronze layer completed
# Run all cells in sequence
```

---

#### Gold Layer (Runtime: <1 minute)

**Files:** `03_Gold_Analysis.py` OR `03_Gold_Analysis.sql`

**What it does:**
- 9 analytical queries for security intelligence:
  1. Yearly CVE trends
  2. Publication latency analysis
  3. Monthly publication patterns
  4. CVSS risk distribution
  5. Top 25 vendors
  6. CVE state distribution
  7. Market concentration
  8. Monthly trends with CVSS
  9. Seasonal vulnerability patterns

**Output:**
- Interactive visualizations (use Databricks built-in charts)
- Security insights and trends

**Run:**
```python
# Requires: Silver layer completed
# Run each analysis cell independently
# Use display() for visualizations
```

---

## Architecture Details

### Bronze Layer
- **Purpose:** Raw data ingestion
- **Input:** CVE v5 JSON in Parquet format
- **Output:** `cve_bronze.records` (Delta table)
- **Key Operations:** Filter to 2024, data quality checks

### Silver Layer
- **Purpose:** Data normalization and cleaning
- **Input:** `cve_bronze.records`
- **Output:** 
  - `cve_silver.core` (one row per CVE)
  - `cve_silver.affected_products` (exploded vendor/product data)
- **Key Operations:** JSON parsing, explode arrays, CVSS coalesce

### Gold Layer
- **Purpose:** Business intelligence and analytics
- **Input:** Silver layer tables
- **Output:** Analytical queries and visualizations
- **Key Operations:** Aggregations, temporal analysis, risk assessment

---

## Key Results

| Metric | Value |
|--------|-------|
| Total CVE Records (2024) | 32,924 |
| Unique Vendors | 1,500+ |
| Vendor/Product Combinations | 50,000+ |
| CVEs with CVSS Scores | 85%+ |
| Avg Products per CVE | ~1.5 |

---

## Data Quality Checks

### Bronze Layer
- ✅ Count >= 30,000 records
- ✅ No null CVE IDs
- ✅ All CVE IDs unique
- ✅ All records from 2024

### Silver Layer
- ✅ Core table has one row per CVE
- ✅ CVSS scores properly coalesced from multiple versions
- ✅ Affected products successfully exploded
- ✅ Proper foreign key relationships maintained

---

## Technologies Used

- **Platform:** Databricks Community Edition (DBR 13.x+)
- **Processing:** Apache Spark / PySpark
- **Storage:** Delta Lake (ACID transactions)
- **Languages:** Python, SQL
- **Architecture:** Medallion (Bronze → Silver → Gold)

---

## Visualizations

All visualizations are available in the `Screenshots/` folder:

1. **Monthly Publication Patterns** - Line chart showing CVE publication trends
2. **CVSS Risk Distribution** - Pie chart of severity levels
3. **Top 25 Vendors** - Horizontal bar chart of vulnerability counts
4. **CVE State Distribution** - Pie chart of Published vs Rejected
5. **Market Concentration** - Analysis of top vendor dominance
6. **Seasonal Patterns** - CVE distribution by season

---

## Execution Proof

Full execution outputs with results are available as PDFs in `ipynb_for_clear_understanding/`:

- `01_Bronze_Ingestion_proof.pdf` - Shows data ingestion and quality checks
- `02_Silver_Normalization_proof.pdf` - Shows transformation and normalization
- `03_Exploratory_Analysis_ricks.pdf` - Shows all analytical queries and insights

---

## Troubleshooting

### Issue: Table not found
**Solution:** Ensure you run notebooks in sequence (01 → 02 → 03)

### Issue: Path not found
**Solution:** Update paths in Cell 1 of each notebook to match your volume name

### Issue: Out of memory
**Solution:** Databricks Community Edition has limited resources; restart cluster if needed

---

## Resume Highlights

**What to include on your resume:**

- Built Bronze, Silver, and Gold data layers using Medallion Architecture on Databricks
- Processed 32,924+ CVE vulnerability records from 2024 using PySpark and Delta Lake
- Implemented data normalization with explode operations on nested JSON structures
- Created business intelligence dashboards for cybersecurity threat analysis
- Demonstrated production-ready data engineering with ACID transactions and data quality checks

---

## Contact

**Author:** [Your Name]  
**Course:** DIC 587 - Data Intensive Computing  
**Semester:** Fall 2025  
**Institution:** [Your University]

---

## Acknowledgments

- CVE data from [CVEProject/cvelistV5](https://github.com/CVEProject/cvelistV5)
- Databricks Community Edition for compute resources
- Assignment designed by DIC 587 course instructors

---

**Last Updated:** November 2025
