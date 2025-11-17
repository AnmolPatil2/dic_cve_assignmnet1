# Databricks notebook source
# MAGIC %md
# MAGIC #CVE LAKEHOUSE - Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Configuration

# COMMAND ----------


from pyspark.sql.functions import col, to_timestamp, year
import time

PARQUET_SOURCE_PATH = "/Volumes/workspace/default/cve_lakehouse_data/2024_parquet.parquet"
TEMP_JSON_DIR = "/Volumes/workspace/default/cve_lakehouse_data/staging_json"
BRONZE_OUTPUT_PATH = "/Volumes/workspace/default/cve_lakehouse_data/bronze"
BRONZE_TABLE_NAME = "cve_lakehouse.bronze_records"
TARGET_YEAR = 2024

spark.conf.set("spark.sql.shuffle.partitions", "8")

print(f"Configuration loaded: {TARGET_YEAR} CVEs")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Parquet and Stage JSON

# COMMAND ----------


start_time = time.time()

print("Reading from Parquet...")
df_raw = spark.read.format("parquet").load(PARQUET_SOURCE_PATH)

print("Converting to JSON format...")
try:
    dbutils.fs.rm(TEMP_JSON_DIR, recurse=True)
except:
    pass

df_raw.select("json_data").write.mode("overwrite").text(TEMP_JSON_DIR)
print("JSON staged successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse JSON to Structured Format

# COMMAND ----------


print("Reading as JSON...")
parquet_read_start = time.time()

df_parsed = spark.read.json(TEMP_JSON_DIR)
raw_count = df_parsed.count()

parquet_read_time = time.time() - parquet_read_start
print(f"Loaded and parsed {raw_count:,} records in {parquet_read_time:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter to 2024 and Quality Checks

# COMMAND ----------


print("Filtering to 2024...")
filter_start = time.time()

date_col = col("cveMetadata.datePublished").cast("string")
df_parsed = df_parsed.withColumn("_datePublished_ts", to_timestamp(date_col))
df_2024 = df_parsed.filter(year(col("_datePublished_ts")) == TARGET_YEAR)

cnt_2024 = df_2024.count()
null_ids = df_2024.filter(col("cveMetadata.cveId").isNull()).count()
distinct_ids = df_2024.select("cveMetadata.cveId").distinct().count()

filter_time = time.time() - filter_start

print(f"\nData Quality Checks:")
print(f"Total: {raw_count:,}")
print(f"2024 filtered: {cnt_2024:,}")
print(f"Null IDs: {null_ids}")
print(f"Distinct IDs: {distinct_ids:,}")

assert cnt_2024 >= 30000, f"Too few rows: {cnt_2024:,}"
assert null_ids == 0, "Null IDs found"
assert distinct_ids == cnt_2024, "IDs not unique"
print("Quality checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write Bronze Delta

# COMMAND ----------


print("\nWriting to Delta...")
delta_start = time.time()

try:
    dbutils.fs.rm(BRONZE_OUTPUT_PATH, recurse=True)
except:
    pass

(df_2024
 .repartition(8)
 .write
 .format("delta")
 .mode("overwrite")
 .option("delta.columnMapping.mode", "name")
 .save(BRONZE_OUTPUT_PATH))

delta_time = time.time() - delta_start
print(f"Delta write completed in {delta_time:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cleanup and Summary

# COMMAND ----------


print("Cleaning up temp files...")
try:
    dbutils.fs.rm(TEMP_JSON_DIR, recurse=True)
except:
    pass

total_time = time.time() - start_time
print(f"\nTiming: Parse+read {parquet_read_time:.2f}s | Filter {filter_time:.2f}s | Delta {delta_time:.2f}s | Total {total_time:.2f}s")
print(f"\nDelta files written to: {BRONZE_OUTPUT_PATH}")
print(f"Records: {cnt_2024:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Register Bronze Table (ALTERNATIVE)

# COMMAND ----------


# Read Delta directly and register as temp view
df_bronze = spark.read.format("delta").load(BRONZE_OUTPUT_PATH)
df_bronze.createOrReplaceTempView("bronze_records")

print(f"Table available as: bronze_records")
print(f"Records: {df_bronze.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Verification

# COMMAND ----------


print("Bronze Layer Verification\n")

# Show sample records
spark.sql("SELECT * FROM bronze_records LIMIT 5").show(truncate=False)

# Show specific columns
spark.sql("""
    SELECT 
        cveMetadata.cveId as CVE_ID,
        cveMetadata.datePublished as Published,
        cveMetadata.state as State
    FROM bronze_records
    LIMIT 10
""").show(truncate=False)

# Final count
final_count = spark.sql("SELECT COUNT(*) as total FROM bronze_records").collect()[0]['total']
print(f"\n🎉 Bronze Layer Complete: {final_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Register Bronze Table with Column Mapping

# COMMAND ----------


# Read existing Delta files
df_bronze = spark.read.format("delta").load(BRONZE_OUTPUT_PATH)

print(f"Delta table contains {df_bronze.count():,} records")

# Create schema if needed
spark.sql("CREATE SCHEMA IF NOT EXISTS cve_bronze")

# Drop existing table
spark.sql("DROP TABLE IF EXISTS cve_bronze.records")

# Create table with column mapping
spark.sql(f"""
CREATE TABLE cve_bronze.records
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT * FROM delta.`{BRONZE_OUTPUT_PATH}`
""")

print("Table registered: cve_bronze.records")

# Verify
spark.sql("SELECT cveMetadata.cveId, cveMetadata.datePublished FROM cve_bronze.records LIMIT 5").show()
spark.sql("SELECT COUNT(*) as total_records FROM cve_bronze.records").show()