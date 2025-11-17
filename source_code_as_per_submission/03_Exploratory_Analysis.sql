-- Databricks notebook source
-- MAGIC %md
-- MAGIC # GOLD LAYER: Exploratory Data Analysis

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import col, count, avg, sum as spark_sum, min as spark_min, max as spark_max, year, month, datediff
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("GOLD LAYER - EXPLORATORY ANALYSIS")
-- MAGIC print("=" * 60)
-- MAGIC print("\nAnalyzing CVE data from Silver layer...")
-- MAGIC print(f"  Source: cve_silver.core")
-- MAGIC print(f"  Source: cve_silver.affected_products")
-- MAGIC print("=" * 60 + "\n")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analysis 1: Temporal Analysis - Yearly CVE Counts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("1. TEMPORAL ANALYSIS - Yearly CVE Counts")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC df_yearly = spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         YEAR(date_published) as year,
-- MAGIC         COUNT(*) as cve_count
-- MAGIC     FROM cve_silver.core
-- MAGIC     WHERE date_published IS NOT NULL
-- MAGIC     GROUP BY YEAR(date_published)
-- MAGIC     ORDER BY year
-- MAGIC """)
-- MAGIC
-- MAGIC display(df_yearly)
-- MAGIC
-- MAGIC print("\n✅ Temporal analysis complete\n")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analysis 2: Publication Latency (Reserved vs Published)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("2. PUBLICATION LATENCY ANALYSIS")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC df_latency = spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         ROUND(AVG(DATEDIFF(date_published, date_reserved)), 2) as avg_days_to_publish,
-- MAGIC         MIN(DATEDIFF(date_published, date_reserved)) as min_days,
-- MAGIC         MAX(DATEDIFF(date_published, date_reserved)) as max_days,
-- MAGIC         COUNT(*) as records_with_both_dates
-- MAGIC     FROM cve_silver.core
-- MAGIC     WHERE date_reserved IS NOT NULL 
-- MAGIC       AND date_published IS NOT NULL
-- MAGIC       AND date_published >= date_reserved
-- MAGIC """)
-- MAGIC
-- MAGIC display(df_latency)
-- MAGIC
-- MAGIC print("\n✅ Publication latency analysis complete\n")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analysis 3: Monthly Publication Patterns (Seasonality)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("3. MONTHLY PUBLICATION PATTERNS")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC df_monthly_patterns = spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         CASE MONTH(date_published)
-- MAGIC             WHEN 1 THEN 'Jan'
-- MAGIC             WHEN 2 THEN 'Feb'
-- MAGIC             WHEN 3 THEN 'Mar'
-- MAGIC             WHEN 4 THEN 'Apr'
-- MAGIC             WHEN 5 THEN 'May'
-- MAGIC             WHEN 6 THEN 'Jun'
-- MAGIC             WHEN 7 THEN 'Jul'
-- MAGIC             WHEN 8 THEN 'Aug'
-- MAGIC             WHEN 9 THEN 'Sep'
-- MAGIC             WHEN 10 THEN 'Oct'
-- MAGIC             WHEN 11 THEN 'Nov'
-- MAGIC             WHEN 12 THEN 'Dec'
-- MAGIC         END as month,
-- MAGIC         COUNT(*) as cve_count
-- MAGIC     FROM cve_silver.core
-- MAGIC     WHERE date_published IS NOT NULL
-- MAGIC     GROUP BY MONTH(date_published)
-- MAGIC     ORDER BY MONTH(date_published)
-- MAGIC """)
-- MAGIC
-- MAGIC display(df_monthly_patterns)
-- MAGIC
-- MAGIC print("\n✅ Monthly patterns analysis complete\n")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analysis 4: CVSS Risk Score Distribution

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # ============================================================
-- MAGIC # Analysis 4: CVSS Risk Distribution
-- MAGIC # ============================================================
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("4. CVSS RISK DISTRIBUTION")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC df_risk = spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         cvss_severity as risk_category,
-- MAGIC         COUNT(*) as cve_count,
-- MAGIC         ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
-- MAGIC     FROM cve_silver.core
-- MAGIC     WHERE cvss_severity IS NOT NULL
-- MAGIC     GROUP BY cvss_severity
-- MAGIC     ORDER BY 
-- MAGIC         CASE cvss_severity
-- MAGIC             WHEN 'CRITICAL' THEN 1
-- MAGIC             WHEN 'HIGH' THEN 2
-- MAGIC             WHEN 'MEDIUM' THEN 3
-- MAGIC             WHEN 'LOW' THEN 4
-- MAGIC         END
-- MAGIC """)
-- MAGIC
-- MAGIC display(df_risk)
-- MAGIC
-- MAGIC print("\n✅ CVSS distribution analysis complete\n")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analysis 5: Top 25 Vendors by Vulnerability Count

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("5. VENDOR INTELLIGENCE - Top 25 Vendors")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC df_top_vendors = spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         vendor,
-- MAGIC         COUNT(DISTINCT cve_id) as vulnerability_count,
-- MAGIC         COUNT(*) as affected_products_count
-- MAGIC     FROM cve_silver.affected_products
-- MAGIC     WHERE vendor IS NOT NULL
-- MAGIC     GROUP BY vendor
-- MAGIC     ORDER BY vulnerability_count DESC
-- MAGIC     LIMIT 25
-- MAGIC """)
-- MAGIC
-- MAGIC display(df_top_vendors)
-- MAGIC
-- MAGIC print("\n✅ Vendor intelligence analysis complete\n")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analysis 6: CVE State Distribution

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("6. CVE STATE DISTRIBUTION")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC df_state = spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         state,
-- MAGIC         COUNT(*) as count,
-- MAGIC         ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
-- MAGIC     FROM cve_silver.core
-- MAGIC     GROUP BY state
-- MAGIC     ORDER BY count DESC
-- MAGIC """)
-- MAGIC
-- MAGIC display(df_state)
-- MAGIC
-- MAGIC print("\n✅ State distribution analysis complete\n")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analysis 7: Market Concentration Index

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("7. MARKET CONCENTRATION - Vendor Analysis")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC df_concentration = spark.sql("""
-- MAGIC     WITH vendor_counts AS (
-- MAGIC         SELECT 
-- MAGIC             vendor,
-- MAGIC             COUNT(DISTINCT cve_id) as cve_count
-- MAGIC         FROM cve_silver.affected_products
-- MAGIC         WHERE vendor IS NOT NULL
-- MAGIC         GROUP BY vendor
-- MAGIC     ),
-- MAGIC     totals AS (
-- MAGIC         SELECT SUM(cve_count) as total_cves FROM vendor_counts
-- MAGIC     ),
-- MAGIC     top10 AS (
-- MAGIC         SELECT SUM(cve_count) as top10_cves 
-- MAGIC         FROM (SELECT cve_count FROM vendor_counts ORDER BY cve_count DESC LIMIT 10)
-- MAGIC     ),
-- MAGIC     top25 AS (
-- MAGIC         SELECT SUM(cve_count) as top25_cves 
-- MAGIC         FROM (SELECT cve_count FROM vendor_counts ORDER BY cve_count DESC LIMIT 25)
-- MAGIC     )
-- MAGIC     SELECT 
-- MAGIC         'Top 10 vendors' as segment,
-- MAGIC         top10_cves as cves,
-- MAGIC         ROUND(top10_cves * 100.0 / total_cves, 2) as percentage
-- MAGIC     FROM top10, totals
-- MAGIC     UNION ALL
-- MAGIC     SELECT 
-- MAGIC         'Top 25 vendors' as segment,
-- MAGIC         top25_cves as cves,
-- MAGIC         ROUND(top25_cves * 100.0 / total_cves, 2) as percentage
-- MAGIC     FROM top25, totals
-- MAGIC     UNION ALL
-- MAGIC     SELECT 
-- MAGIC         'All vendors' as segment,
-- MAGIC         total_cves as cves,
-- MAGIC         100.0 as percentage
-- MAGIC     FROM totals
-- MAGIC """)
-- MAGIC
-- MAGIC display(df_concentration)
-- MAGIC
-- MAGIC print("\n✅ Market concentration analysis complete\n")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analysis 8: Monthly Publication Trends 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("8. MONTHLY TRENDS - 2024 CVE Count & Average CVSS")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC df_monthly = spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         CASE MONTH(date_published)
-- MAGIC             WHEN 1 THEN 'Jan'
-- MAGIC             WHEN 2 THEN 'Feb'
-- MAGIC             WHEN 3 THEN 'Mar'
-- MAGIC             WHEN 4 THEN 'Apr'
-- MAGIC             WHEN 5 THEN 'May'
-- MAGIC             WHEN 6 THEN 'Jun'
-- MAGIC             WHEN 7 THEN 'Jul'
-- MAGIC             WHEN 8 THEN 'Aug'
-- MAGIC             WHEN 9 THEN 'Sep'
-- MAGIC             WHEN 10 THEN 'Oct'
-- MAGIC             WHEN 11 THEN 'Nov'
-- MAGIC             WHEN 12 THEN 'Dec'
-- MAGIC         END as month,
-- MAGIC         COUNT(*) as cve_count,
-- MAGIC         ROUND(AVG(cvss_base_score), 2) as avg_cvss_score
-- MAGIC     FROM cve_silver.core
-- MAGIC     WHERE YEAR(date_published) = 2024 
-- MAGIC       AND cvss_base_score IS NOT NULL
-- MAGIC     GROUP BY MONTH(date_published)
-- MAGIC     ORDER BY MONTH(date_published)
-- MAGIC """)
-- MAGIC
-- MAGIC display(df_monthly)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analysis 9: Seasonal Vulnerability Patterns

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("9. SEASONAL ANALYSIS - CVE Patterns by Season")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC df_seasonal = spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         CASE 
-- MAGIC             WHEN MONTH(date_published) IN (12, 1, 2) THEN 'Winter'
-- MAGIC             WHEN MONTH(date_published) IN (3, 4, 5) THEN 'Spring'
-- MAGIC             WHEN MONTH(date_published) IN (6, 7, 8) THEN 'Summer'
-- MAGIC             ELSE 'Fall'
-- MAGIC         END as season,
-- MAGIC         COUNT(*) as cve_count,
-- MAGIC         ROUND(AVG(cvss_base_score), 2) as avg_severity
-- MAGIC     FROM cve_silver.core
-- MAGIC     WHERE cvss_base_score IS NOT NULL
-- MAGIC     GROUP BY season
-- MAGIC     ORDER BY 
-- MAGIC         CASE season
-- MAGIC             WHEN 'Winter' THEN 1
-- MAGIC             WHEN 'Spring' THEN 2
-- MAGIC             WHEN 'Summer' THEN 3
-- MAGIC             ELSE 4
-- MAGIC         END
-- MAGIC """)
-- MAGIC
-- MAGIC display(df_seasonal)
-- MAGIC
-- MAGIC print("\n✅ Seasonal analysis complete\n")