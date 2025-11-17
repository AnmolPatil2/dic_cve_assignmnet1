# CVE Lakehouse on Databricks - Medallion Architecture

## Project Overview

A cybersecurity data lakehouse analyzing **32,924 CVE records from 2024** using Databricks and Delta Lake. Implements the Medallion Architecture (Bronze → Silver → Gold) for vulnerability intelligence.

**Course:** DIC 587 - Data Intensive Computing | Fall 2025  
**Assignment:** CVE Lakehouse Implementation  
**Due Date:** November 16, 2025

---

## Project Structure
```
dic_cve_assignmnet1/
│
├── README.md
│
├── data/
│   └── 2024_parquet.parquet          # CVE 2024 source data (32,924 records)
│
├── source_code_as_per_submission/    # Python/SQL files for submission
│   ├── 01_Bronze_Layer.py            # Bronze layer ingestion
│   ├── 02_Silver_Normalization.py    # Silver layer normalization
│   ├── 03_Gold_Analysis.py           # Gold layer analytics (Python)
│   └── 03_Gold_Analysis.sql          # Gold layer analytics (SQL)
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
└── ipynb_for_clear_understanding/   # Jupyter notebooks (.ipynb) for all 3 layers
    ├── 01_Bronze_Layer.ipynb
    ├── 02_Silver_Normalization.ipynb
    └── 03_Gold_Analysis.ipynb
```

---

## Complete Setup Instructions

### Step 1: Databricks Account Setup

1. **Create Databricks Community Edition Account:**
   - Navigate to: https://community.cloud.databricks.com/
   - Click "Sign up for free"
   - Use Google account (requires 2FA enabled)
   - Complete registration

2. **Create and Configure Cluster:**
   - Go to "Compute" in left sidebar
   - Click "Create Cluster"
   - **Cluster Settings:**
     - Cluster name: `CVE-Lakehouse-Cluster` (or any preferred name)
     - Cluster mode: Single Node
     - Databricks runtime version: **13.x LTS or newer** (recommended: 13.3 LTS)
     - Node type: Default (Community Edition provides standard configuration)
     - Auto-termination: 120 minutes (recommended)
   - Click "Create Cluster"
   - **Wait ~3-5 minutes for cluster to start**
   - Status will show "Running" when ready

---

### Step 2: Volume Setup

**Create Unity Catalog Volume for Data Storage:**

1. **Open SQL Editor:**
   - Click "SQL Editor" in left sidebar
   - Or navigate: "Workspace" → Create → SQL Query

2. **Create Volume:**
```sql
   CREATE VOLUME IF NOT EXISTS workspace.default.cve_lakehouse_data;
```
   
3. **Verify Volume Created:**
```sql
   SHOW VOLUMES IN default;
```
   - Expected output: Should list `cve_lakehouse_data` volume

**Volume Path Details:**
- **Full Path:** `/Volumes/workspace/default/cve_lakehouse_data/`
- **Catalog:** `workspace`
- **Schema:** `default`
- **Volume Name:** `cve_lakehouse_data`

---

### Step 3: Upload Source Data

1. **Navigate to Volume:**
   - Click "Data" in left sidebar
   - Expand: `workspace` → `default` → `cve_lakehouse_data`

2. **Upload Parquet File:**
   - Click "Upload" or "Upload files" button
   - Select `data/2024_parquet.parquet` from this repository
   - Wait for upload progress to complete

3. **Verify Upload Success:**
   - File `2024_parquet.parquet` should appear in volume file list
   - **Expected Path:** `/Volumes/workspace/default/cve_lakehouse_data/2024_parquet.parquet`
   - Click on file to view metadata and confirm successful upload

---

### Step 4: Import Notebooks

**Option A: Import Python Files (Recommended for Databricks)**

1. Go to "Workspace" in left sidebar
2. Navigate to your user folder
3. Right-click on folder → "Import"
4. Select "File" import option
5. Import each file from `source_code_as_per_submission/`:
   - `01_Bronze_Layer.py`
   - `02_Silver_Normalization.py`
   - `03_Gold_Analysis.py`
   - `03_Gold_Analysis.sql`

**Option B: Import Jupyter Notebooks**

1. Same process as above
2. Import `.ipynb` files from `ipynb_for_clear_understanding/` folder
3. Files will be recognized as notebooks automatically

**Attach Notebooks to Cluster:**
- Open each imported notebook
- Click cluster dropdown in top-right corner
- Select your cluster (`CVE-Lakehouse-Cluster`)
- Wait for "Connected" status indicator

---

## Pipeline Execution Guide

### ⚠️ CRITICAL: Execute in This Exact Order
```
Step 5: 01_Bronze_Layer → Step 6: 02_Silver_Normalization → Step 7: 03_Gold_Analysis
```

Each layer depends on the previous layer's output tables.

---

### 🥉 Step 5: Bronze Layer Execution

**File:** `01_Bronze_Layer.py` or `01_Bronze_Layer.ipynb`  
**Estimated Runtime:** 2-3 minutes  
**Cluster Status Required:** Running

#### What This Layer Does:
- Reads raw CVE data from Parquet file
- Filters records to 2024 only by publication date
- Performs data quality validation checks
- Creates immutable Bronze Delta table
- Establishes data lineage and audit trail

#### Execution Steps:

1. **Open Notebook:** `01_Bronze_Layer` in Databricks workspace
2. **Review Configuration (Cell 1):**
```python
   PARQUET_SOURCE_PATH = "/Volumes/workspace/default/cve_lakehouse_data/2024_parquet.parquet"
   BRONZE_OUTPUT_PATH = "/Volumes/workspace/default/cve_lakehouse_data/bronze"
   BRONZE_TABLE_NAME = "cve_bronze.records"
   TARGET_YEAR = 2024
```
3. **Execute Pipeline:** 
   - Click "Run All" button at top of notebook
   - OR run cells sequentially from top to bottom
4. **Monitor Progress:** 
   - Watch for completion messages in each cell
   - Check for green checkmarks indicating successful execution

#### Expected Outputs & Verification:

| Checkpoint | Expected Result | Verification Command |
|------------|-----------------|---------------------|
| **Records Loaded** | 32,924 | Check cell output |
| **Table Created** | `cve_bronze.records` | `SHOW TABLES IN cve_bronze;` |
| **Table Location** | `/Volumes/.../bronze` | `DESCRIBE DETAIL cve_bronze.records;` |
| **Null CVE IDs** | 0 | Cell output shows assertion passed |
| **Unique CVE IDs** | 32,924 (100% unique) | Cell output shows assertion passed |
| **Data Quality** | All checks PASSED ✅ | Watch for assertion success messages |

#### Verification SQL Queries:
```sql
-- Verify record count
SELECT COUNT(*) as total_records FROM cve_bronze.records;
-- Expected: 32,924

-- Check for null IDs
SELECT COUNT(*) as null_ids 
FROM cve_bronze.records 
WHERE cveMetadata.cveId IS NULL;
-- Expected: 0

-- Verify uniqueness
SELECT COUNT(DISTINCT cveMetadata.cveId) as unique_ids 
FROM cve_bronze.records;
-- Expected: 32,924

-- Sample records
SELECT cveMetadata.cveId, cveMetadata.datePublished, cveMetadata.state 
FROM cve_bronze.records 
LIMIT 5;

-- Table details
DESCRIBE DETAIL cve_bronze.records;
```

#### Troubleshooting:

- **Error: "Path not found"** → Verify Parquet file uploaded to exact path in Step 3
- **Error: "Volume does not exist"** → Re-run Step 2 volume creation command
- **Error: "Permission denied"** → Ensure cluster is running and notebook is attached
- **Row count < 32,924** → Check `TARGET_YEAR` is set to 2024 in configuration
- **Cluster timeout** → Restart cluster and re-run notebook

---

### 🥈 Step 6: Silver Layer Execution

**File:** `02_Silver_Normalization.py` or `02_Silver_Normalization.ipynb`  
**Estimated Runtime:** 1-2 minutes  
**Prerequisites:** Bronze layer (`cve_bronze.records`) must exist

#### What This Layer Does:
- Reads from Bronze layer table
- Normalizes nested JSON into flat relational structure
- Extracts core CVE fields (ID, dates, CVSS scores, descriptions)
- Handles multiple CVSS versions with coalesce logic
- **Explodes** nested vendor/product arrays (one-to-many relationships)
- Creates two clean, queryable Silver tables

#### Execution Steps:

1. **Verify Bronze Prerequisite:**
```sql
   SELECT COUNT(*) FROM cve_bronze.records;  -- Must return 32,924
```

2. **Open Notebook:** `02_Silver_Normalization`

3. **Review Configuration (Cell 1):**
```python
   BRONZE_DELTA_PATH = "/Volumes/workspace/default/cve_lakehouse_data/bronze"
   SILVER_CORE_PATH = "/Volumes/workspace/default/cve_lakehouse_data/silver/core"
   SILVER_AFFECTED_PATH = "/Volumes/workspace/default/cve_lakehouse_data/silver/affected_products"
   SILVER_CORE_TABLE = "cve_silver.core"
   SILVER_AFFECTED_TABLE = "cve_silver.affected_products"
```

4. **Execute:** Click "Run All" or run cells sequentially

5. **Watch for Table Creation Messages:**
   - "Core records extracted: 32,924"
   - "Affected products extracted: 50,000+"
   - "✅ Core table created"
   - "✅ Affected products table created"

#### Expected Outputs & Verification:

**Core Table (`cve_silver.core`):**

| Attribute | Expected Value |
|-----------|---------------|
| **Table Name** | `cve_silver.core` |
| **Table Path** | `/Volumes/.../silver/core` |
| **Row Count** | 32,924 (one row per unique CVE) |
| **Key Columns** | `cve_id`, `state`, `date_published`, `date_reserved`, `date_updated`, `cvss_base_score`, `cvss_severity`, `description` |

**Affected Products Table (`cve_silver.affected_products`):**

| Attribute | Expected Value |
|-----------|---------------|
| **Table Name** | `cve_silver.affected_products` |
| **Table Path** | `/Volumes/.../silver/affected_products` |
| **Row Count** | 50,000+ (exploded vendor/product combinations) |
| **Key Columns** | `cve_id`, `vendor`, `product`, `versions` |
| **Unique Vendors** | 1,500+ |

#### Verification SQL Queries:
```sql
-- Verify core table count
SELECT COUNT(*) as total_cves FROM cve_silver.core;
-- Expected: 32,924

-- Verify affected products count
SELECT COUNT(*) as total_combinations FROM cve_silver.affected_products;
-- Expected: 50,000+

-- Count unique vendors
SELECT COUNT(DISTINCT vendor) as unique_vendors 
FROM cve_silver.affected_products 
WHERE vendor IS NOT NULL;
-- Expected: 1,500+

-- Verify no duplicate CVEs in core
SELECT cve_id, COUNT(*) as count 
FROM cve_silver.core 
GROUP BY cve_id 
HAVING COUNT(*) > 1;
-- Expected: 0 rows (no duplicates)

-- Sample core table data
SELECT cve_id, state, date_published, cvss_base_score, cvss_severity 
FROM cve_silver.core 
LIMIT 5;

-- Sample affected products with vendor info
SELECT cve_id, vendor, product 
FROM cve_silver.affected_products 
WHERE vendor IS NOT NULL 
LIMIT 10;

-- Check table schemas
DESCRIBE TABLE cve_silver.core;
DESCRIBE TABLE cve_silver.affected_products;

-- Verify join relationship works
SELECT c.cve_id, c.cvss_severity, a.vendor, a.product
FROM cve_silver.core c
JOIN cve_silver.affected_products a ON c.cve_id = a.cve_id
WHERE c.cvss_severity = 'CRITICAL'
LIMIT 5;
```

#### Troubleshooting:

- **Error: "Table cve_bronze.records not found"** → Complete Bronze layer (Step 5) first
- **Low affected products count** → Verify explode operation in Cell 3 executed successfully
- **Missing vendor data** → Normal, some CVEs don't have vendor information
- **Tables not persisting** → Community Edition limitation; tables may need recreation after cluster restart

---

### 🥇 Step 7: Gold Layer Execution

**File:** `03_Gold_Analysis.py`, `03_Gold_Analysis.sql`, or `03_Gold_Analysis.ipynb`  
**Estimated Runtime:** <1 minute per query  
**Prerequisites:** Both Silver tables must exist

#### What This Layer Does:
- Performs 9 comprehensive security intelligence analyses
- Generates interactive visualizations using Databricks charts
- Provides temporal trends, risk distributions, and vendor intelligence
- Creates business-ready insights for cybersecurity decision-making

#### Execution Steps:

1. **Verify Silver Prerequisites:**
```sql
   SELECT COUNT(*) FROM cve_silver.core;              -- Must return 32,924
   SELECT COUNT(*) FROM cve_silver.affected_products; -- Must return 50,000+
```

2. **Open Notebook:** `03_Gold_Analysis` (choose Python, SQL, or ipynb version)

3. **Execute Queries:**
   - Run each analysis cell individually
   - Review results after each query
   - Click chart/visualization icons to create graphs

4. **Create Visualizations:**
   - After running each query, click the chart icon (bar/pie/line)
   - Configure chart settings (X-axis, Y-axis, chart type)
   - Save or export charts as needed

#### Analysis Queries Reference:

| Analysis # | Description | Expected Output Type |
|-----------|-------------|---------------------|
| **1** | Yearly CVE publication trends | Bar chart showing 2024 count |
| **2** | Publication latency (reserved → published) | Summary statistics table |
| **3** | Monthly publication patterns | Line chart with 12 data points |
| **4** | CVSS risk distribution by severity | Pie chart with 4 categories |
| **5** | Top 25 vendors by vulnerability count | Horizontal bar chart |
| **6** | CVE state distribution (Published vs Rejected) | Pie chart with 2 categories |
| **7** | Market concentration (Top 10/25 vendors) | Table with percentages |
| **8** | Monthly trends with average CVSS scores | Combo chart (bars + line) |
| **9** | Seasonal vulnerability patterns | Bar chart with 4 seasons |

#### Sample Verification Queries:

**Analysis 1: Yearly Trends**
```sql
SELECT YEAR(date_published) as year, COUNT(*) as cve_count
FROM cve_silver.core
WHERE date_published IS NOT NULL
GROUP BY YEAR(date_published)
ORDER BY year;
-- Expected: Single row showing 2024 with 32,924 count
```

**Analysis 4: CVSS Risk Distribution**
```sql
SELECT 
    cvss_severity as risk_category,
    COUNT(*) as cve_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM cve_silver.core
WHERE cvss_severity IS NOT NULL
GROUP BY cvss_severity
ORDER BY 
    CASE cvss_severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
    END;
-- Expected: 4 rows (CRITICAL, HIGH, MEDIUM, LOW) with counts
```

**Analysis 5: Top Vendors**
```sql
SELECT 
    vendor,
    COUNT(DISTINCT cve_id) as vulnerability_count
FROM cve_silver.affected_products
WHERE vendor IS NOT NULL
GROUP BY vendor
ORDER BY vulnerability_count DESC
LIMIT 25;
-- Expected: 25 rows with vendor names and vulnerability counts
```

#### Troubleshooting:

- **Error: "Table not found"** → Verify Silver layer (Step 6) completed successfully
- **Empty results** → Run `SELECT COUNT(*) FROM cve_silver.core` to verify data exists
- **Charts not displaying** → Use `display()` function (Python) or click chart icon (SQL results)
- **Query timeout** → Normal for Community Edition; try running queries individually

---

## Complete Reference Tables

### Table Names and Paths

| Layer | Table Name | Full Path | Row Count |
|-------|-----------|-----------|-----------|
| **Bronze** | `cve_bronze.records` | `/Volumes/workspace/default/cve_lakehouse_data/bronze` | 32,924 |
| **Silver** | `cve_silver.core` | `/Volumes/workspace/default/cve_lakehouse_data/silver/core` | 32,924 |
| **Silver** | `cve_silver.affected_products` | `/Volumes/workspace/default/cve_lakehouse_data/silver/affected_products` | 50,000+ |

### Quick Verification Commands
```sql
-- Check all tables exist
SHOW TABLES IN cve_bronze;
SHOW TABLES IN cve_silver;

-- Verify all row counts
SELECT 'Bronze' as layer, COUNT(*) as rows FROM cve_bronze.records
UNION ALL
SELECT 'Silver Core', COUNT(*) FROM cve_silver.core
UNION ALL
SELECT 'Silver Products', COUNT(*) FROM cve_silver.affected_products;
```

---

## Key Results & Insights

### Data Processing Metrics

| Metric | Value | Significance |
|--------|-------|--------------|
| **Total CVE Records (2024)** | 32,924 | Record-breaking year for vulnerability disclosures |
| **Unique Vendors Affected** | 1,500+ | Diverse ecosystem of affected organizations |
| **Vendor/Product Combinations** | 50,000+ | Result of exploding nested arrays (one-to-many) |
| **CVEs with CVSS Scores** | 85%+ | Majority of vulnerabilities have severity ratings |
| **Avg Products per CVE** | ~1.5 | Single vulnerability often affects multiple products |

### Security Intelligence Insights

**Risk Distribution Categories:**
- **CRITICAL** (9.0-10.0): Highest severity, requires immediate patching
- **HIGH** (7.0-8.9): Serious vulnerabilities, prompt action needed
- **MEDIUM** (4.0-6.9): Moderate risk, schedule patching
- **LOW** (0.1-3.9): Lower priority, monitor for updates

**Vendor Intelligence Value:**
- Identifies most vulnerable vendors/products
- Market concentration reveals if risk is distributed or concentrated
- Helps prioritize security efforts and vendor relationships
- Supports supply chain risk assessment

**Temporal Pattern Analysis:**
- Monthly trends reveal disclosure patterns
- Seasonal analysis shows cyclical vulnerability discovery
- Publication latency indicates time-to-disclosure metrics
- Supports predictive security planning

---

## Documentation Files

### Execution Proof PDFs (Screenshots/ folder)

Complete outputs demonstrating successful pipeline execution:

- **`01_Bronze_Ingestion_proof.pdf`**  
  Contains: Data ingestion logs, filtering results, quality checks, Bronze table creation confirmation

- **`02_Silver_Normalization_proof.pdf`**  
  Contains: JSON parsing steps, explode operations, both Silver table creations, verification queries

- **`03_Exploratory_Analysis_ricks.pdf`**  
  Contains: All 9 Gold layer analyses with results, charts, and insights

### Analysis Visualizations (Screenshots/ folder)

6 PNG chart images showing:
1. Monthly CVE publication patterns (line chart)
2. CVSS severity distribution (pie chart)
3. Top 25 vendors by vulnerability count (horizontal bar)
4. CVE state: Published vs Rejected (pie chart)
5. Market concentration analysis (table)
6. Monthly trends with CVSS averages (combo chart)

### Jupyter Notebooks (ipynb_for_clear_understanding/ folder)

Alternative `.ipynb` format for all 3 pipeline layers:
- `01_Bronze_Layer.ipynb` - Bronze ingestion notebook
- `02_Silver_Normalization.ipynb` - Silver transformation notebook
- `03_Gold_Analysis.ipynb` - Gold analytics notebook

---

## Architecture Overview

**Bronze Layer (Raw Ingestion)**
- Purpose: Immutable source of truth
- Input: CVE v5 JSON in Parquet format
- Processing: Filter, validate, preserve original structure
- Output: `cve_bronze.records` Delta table

**Silver Layer (Normalization)**
- Purpose: Cleaned, relational data model
- Input: `cve_bronze.records`
- Processing: Parse JSON, extract fields, explode arrays, create relationships
- Output: `cve_silver.core` + `cve_silver.affected_products` Delta tables

**Gold Layer (Analytics)**
- Purpose: Business intelligence and security insights
- Input: Silver layer tables
- Processing: Aggregations, joins, temporal analysis, risk scoring
- Output: 9 analytical queries with visualizations

---

## Technologies Stack

| Category | Technology | Version/Details |
|----------|-----------|-----------------|
| **Platform** | Databricks Community Edition | DBR 13.x+ recommended |
| **Processing** | Apache Spark / PySpark | Distributed data processing |
| **Storage** | Delta Lake | ACID transactions, time travel |
| **Languages** | Python & SQL | Notebook-based development |
| **Data Format** | JSON, Parquet, Delta | CVE v5 schema compliance |
| **Architecture** | Medallion (Bronze→Silver→Gold) | Industry standard pattern |

---

## Common Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| "Table not found" | Pipeline run out of order | Execute notebooks sequentially: 01 → 02 → 03 |
| "Path not found" | Data file not uploaded | Verify `2024_parquet.parquet` in volume at exact path |
| "Volume does not exist" | Volume not created | Run `CREATE VOLUME` SQL command in Step 2 |
| "Permission denied" | Notebook not attached to cluster | Attach notebook to running cluster |
| "Out of memory" | Community Edition resource limits | Restart cluster, reduce data sample, or optimize queries |
| Row count mismatch | Wrong year filter | Verify `TARGET_YEAR = 2024` in Bronze configuration |
| Tables not persisting | Community Edition limitation | Normal behavior; re-run pipeline if needed after restart |
| Slow query performance | Community Edition resources | Expected; optimize with filters and limits |

---

## Assignment Deliverables Checklist

✅ **GitHub Repository** - Public repo with complete code and documentation  
✅ **Bronze Layer** - 32,924 records, filtered to 2024, all quality checks passed  
✅ **Silver Layer** - Two normalized tables with explode operations implemented  
✅ **Gold Layer** - 9 comprehensive analyses with visualizations  
✅ **Screenshots** - Execution proofs (PDFs) and analysis charts (PNGs)  
✅ **Source Data** - Parquet file included in `data/` folder for reproducibility  
✅ **Documentation** - Complete README with setup and verification instructions  
✅ **Alternative Formats** - Both `.py` and `.ipynb` versions provided  

---

## Repository Information

**GitHub URL:** https://github.com/AnmolPatil2/dic_cve_assignmnet1  
**Course:** DIC 587 - Data Intensive Computing  
**Semester:** Fall 2025  
**Assignment:** CVE Lakehouse Implementation  
**Due Date:** November 16, 2025

---

## Data Sources & References

**CVE Data Source:** [CVEProject/cvelistV5](https://github.com/CVEProject/cvelistV5)  
**Platform:** Databricks Community Edition  
**Documentation:** [Databricks Delta Lake Docs](https://docs.databricks.com/delta/)  
**Architecture Pattern:** [Medallion Architecture](https://docs.databricks.com/lakehouse/medallion.html)

---

**Last Updated:** November 16, 2025
