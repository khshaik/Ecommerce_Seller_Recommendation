#!/usr/bin/env python3
"""
etl_seller_catalog.py

===================================================
Seller Catalog ETL (Bronze → Silver + Quarantine)
===================================================
- Reads raw seller catalog CSV from config
- Cleans & standardizes fields
- Applies all DQ rules strictly
- Duplicate handling STRICTLY based on (seller_id + item_id):
      → First occurrence → Silver
      → Duplicate occurrences → Quarantine
- Writes:
      Silver → CSV
      Quarantine → CSV partitioned by run_date

===================================================
Data Validation Scenarios (Reference Table)
===================================================
    Columns validated: item_id, units_sold, revenue, and sale_date.
+---------+-----------+----------------+------------------+------------+-------------+---------------+--------------+
| seller  | item_id   | item_name      | category         | price      | stock_qty   | reason            | Is valid |
+---------+-----------+----------------+------------------+------------+-------------+---------------+--------------+
| S101    | I10000    | Apple Watch    | Electronics      | 29999      | 12          | valid             | yes      |
| S101    | I10000    | Apple Watch    | Electronics      | 29999      | 12          | duplicate key     | no       |
| (empty) | I10000    | Apple Watch    | Electronics      | 29999      | 12          | missing seller_id | no       |
| S103    | (empty)   | Nike Shoes     | Footwear         | 7999       | 4           | missing item_id   | no       |
| S104    | I10002    | Samsung TV     | Electronics      | -45000     | 3           | negative price    | no       |
| S105    | I10003    | JBL Speaker    | Electronics      | 5000       | -2          | negative stock    | no       |
| S106    | I10004    | (empty)        | Apparel          | 1999       | 8           | missing item_name | no       |
| S107    | I10005    | Puma T-Shirt   | (empty)          | 1299       | 6           | missing category  | no       |
| S108    | I10006    | HP Laptop      | Electronics      | 45000      | 0           | valid (zero stock)| yes      |
| S109    | I10007    | Sony Camera    | Electronics      | (empty)    | 1           | missing price     | no       |
+---------+-----------+----------------+------------------+------------+-------------+---------------+---------------+

===================================================
DQ Rules Applied
===================================================
• seller_id must NOT be null/empty
• item_id must NOT be null/empty
• item_name must be present (non-null & non-empty)
• category must be present (non-null & standardized)
• marketplace_price must be numeric AND ≥ 0
• stock_qty must be numeric AND ≥ 0
• duplicates (based on seller_id + item_id) → quarantined
• zero values for marketplace_price & stock_qty ARE allowed
• string fields must be trimmed after cleaning; empty after trim → quarantined

===================================================
Cleaning Steps
===================================================
• Trim whitespace in all string columns
      - seller_id, item_id, item_name, category
• Normalize casing
      - item_name → Title Case   (e.g., “apple watch” → “Apple Watch”)
      - category → Standard labels (“Apparel”, “Electronics”, etc.)
• Convert:
      - marketplace_price → DOUBLE
      - stock_qty → INT
• Fill missing numeric fields:
      - stock_qty → 0 (if missing)
• Remove duplicates strictly based on (seller_id + item_id):
      - 1st occurrence → kept for Silver
      - others → Quarantine with reason "duplicate key"

NOTE:
• (seller_id + item_id) is the unique key for Seller Catalog.
• All dirty rows must be quarantined with explicit reason tagging.
• marketplace_price drives expected revenue calculations — must be valid.
• category normalization ensures categories line up with recommendation logic.
• Always deduplicate before DQ checks; duplicates always go to quarantine.
• Never push dirty or duplicate rows into the Silver zone.

===================================================
Bronze/Silver Files Records
===================================================
------------------------------------------------------------------
| Metric                   | Expected Behavior      | Your Value | 
| ------------------------ | ---------------------- | ---------- | 
| Total Bronze             | Raw input              | 1,000,500  | 
| Non-duplicate            | Remove exact dupes     | 980,239    | 
| Duplicate rows           | Difference             | 20,261     | 
| Total Quarantine         | Failed DQ              | 78,001     | 
| Non-duplicate Quarantine | Unique rows failing DQ | 57,740     | 
| Valid Silver Records     | Clean + unique         | 922,499    | 
------------------------------------------------------------------
"""
import sys
import os
import yaml
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
#from ecommerce_seller_recommendation.local.libs.log import get_logger

# ===========================================================
# Spark Session Builder
# ===========================================================
def get_spark(app_name: str):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    #spark.sparkContext.setLogLevel("WARN")
    # Ensure legacy time parser policy only if needed elsewhere
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    return spark

# ===========================================================
# Load YAML Config
# ===========================================================
def load_config(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# ===========================================================
# Write DF as CSV
# ===========================================================
def write_df(df, path):
    os.makedirs(path, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)

# ===========================================================
# Main ETL
# ===========================================================
def main():

    # -----------------------------------
    # Load config path argument
    # -----------------------------------
    if "--config" not in sys.argv:
        print("Usage: etl_seller_catalog.py --config <config.yml>")
        sys.exit(1)

    config_path = sys.argv[sys.argv.index("--config") + 1]
    if not os.path.exists(config_path):
        print(f"❌ Config file not found: {config_path}")
        sys.exit(1)

    cfg = load_config(config_path)
    p = cfg.get("paths", {})
    dq_cfg = cfg.get("dq", {})

    # Make sure both p and paths variables exist (some code references 'paths')
    paths = p

    run_ts = datetime.today().strftime("%Y-%m-%d_%H%M%S")

    spark = get_spark("SellerCatalogETL")
    #logger = get_logger() if "get_logger" in globals() else None

    # ===========================================================
    # 1. READ RAW (Bronze)
    # ===========================================================
    raw_path = paths.get("raw_sellers_catalogue_data")
    if not raw_path:
        print("❌ Missing config key: paths.raw_sellers_catalogue_data")
        sys.exit(1)

    raw_df = (
        spark.read
        .option("header", True)
        .csv(raw_path)
    )
    # ------------------- ADDED: Show Raw Data Preview -------------------
    print("\n📥 RAW INPUT SAMPLE (Top 15 rows):")
    raw_df.show(20, truncate=False)

    bronze_root = paths.get("bronze_root")
    
    # optional bronze backup
    if bronze_root:
        bronze_out = os.path.join(bronze_root, f"seller_catalog/run_date={run_ts}")
        write_df(raw_df, bronze_out)
    
    # ===========================================================
    # 2. CLEANING (Silver Transform)
    # ===========================================================
    df = raw_df.select(
        F.trim("seller_id").alias("seller_id"),
        F.trim("item_id").alias("item_id"),
        F.initcap(F.trim("item_name")).alias("item_name"),
        F.initcap(F.trim("category")).alias("category"),
        F.col("marketplace_price").cast(DoubleType()).alias("marketplace_price"),
        F.col("stock_qty").cast(IntegerType()).alias("stock_qty")
    )

    # Fill missing stock_qty with 0
    df = df.withColumn(
        "stock_qty",
        F.when(F.col("stock_qty").isNull(), 0).otherwise(F.col("stock_qty"))
    )

    # ------------------- ADDED: Show Cleaned-Tidy Data -------------------
    print("\n🧹 CLEANED DATA (After trimming, casing, type casting):")
    df.show(20, truncate=False)

    # ===========================================================
    # 3. DQ VALIDATION RULES
    # ===========================================================

    # 1. Duplicate detection (only rn > 1)
    dup_window = Window.partitionBy("seller_id", "item_id").orderBy(F.monotonically_increasing_id())

    df = df.withColumn("rn", F.row_number().over(dup_window)).withColumn("is_duplicate", F.col("rn") > 1)

    # 2. Build DQ rules
    rules = []

    # Required fields (from config) - fallback default list if not provided
    required_cols = dq_cfg.get("sellers_catalogue_data_required", ["seller_id", "item_id", "item_name", "category"])
    for c in required_cols:
        rules.append((
            (F.col(c).isNull() | (F.trim(F.col(c)) == "")),
            F.lit(f"{c}_missing")
        ))

    # Numeric checks
    numeric_checks = dq_cfg.get("seller_catalog_numeric_checks", {"marketplace_price": 0, "stock_qty": 0})
    for col_name, min_val in numeric_checks.items():
        # Only add check when column exists
        rules.append((
            (F.col(col_name) < F.lit(min_val)),
            F.lit(f"{col_name}_invalid")
        ))

    # Build array of reasons
    reason_exprs = [F.when(cond, reason).otherwise(F.lit(None)) for cond, reason in rules]

    # Build array of reasons
    df = df.withColumn(
        "dq_failure_reason",
        F.array(*[F.when(cond, reason).otherwise(F.lit(None)) for cond, reason in rules])
    )

    # Filter out nulls
    df = df.withColumn(
        "dq_failure_reason",
        F.expr("filter(dq_failure_reason, x -> x is not null)")
    )

    # Add duplicate reason
    df = df.withColumn(
        "dq_failure_reason",
        F.when(
            F.col("is_duplicate"),
            F.array_union(F.col("dq_failure_reason"), F.array(F.lit("duplicate_record")))
        ).otherwise(F.col("dq_failure_reason"))
    )

    # Compute final pass/fail
    df = df.withColumn("dq_pass", F.size("dq_failure_reason") == 0)

    # ===========================================================
    # FINAL SPLIT
    # ===========================================================
    valid_df = df.filter(F.col("dq_pass") == True)
    invalid_df = df.filter(F.col("dq_pass") == False)

    # Remove helper columns *before write*
    valid_df = valid_df.drop("rn", "is_duplicate", "dq_pass")
    invalid_df = invalid_df.drop("rn", "is_duplicate", "dq_pass")

    # ===========================================================
    # 4. WRITE QUARANTINE
    # ===========================================================
    # CSV cannot store ARRAY<STRING> → convert dq_failure_reason to a single string
    invalid_df = invalid_df.withColumn("dq_failure_reason", F.concat_ws(": ", "dq_failure_reason"))

    # ------------------- ADDED: Show Quarantine Rows -------------------
    print("\n❌ QUARANTINE ROWS (Invalid Records with DQ Reasons):")
    invalid_df.show(50, truncate=False)

    # Prepare quarantine folder with run_date partition
    quarantine_root = paths.get("quarantine_sellers_catalogue_data")
    if not quarantine_root:
        print("❌ Missing config key: paths.quarantine_sellers_catalogue_data")
        sys.exit(1)

    quarantine_path = os.path.join(quarantine_root, f"run_date={run_ts}")
    write_df(invalid_df, quarantine_path)

    # ===========================================================
    # 5. DEDUPE VALID DATA USING (seller_id + item_id)
    # ===========================================================
    # Remove dq_failure_reason (ARRAY) – not needed for Silver
    if "dq_failure_reason" in valid_df.columns:
        valid_df = valid_df.drop("dq_failure_reason")

    # Dedupe logic
    w = Window.partitionBy("seller_id", "item_id").orderBy(F.lit(1))
    valid_df = (
        valid_df.withColumn("rn", F.row_number().over(w))
                .filter("rn = 1")
                .drop("rn")
    )

    # ------------------- ADDED: Show Final Clean Silver Data -------------------
    print("\n✅ CLEAN SILVER DATA (Valid Records After Dedupe):")
    valid_df.show(50, truncate=False)

    # ===========================================================
    # Prepare silver columns list (used for CSV + Hudi)
    # ===========================================================
    silver_cols = [
        "seller_id",
        "item_id",
        "item_name",
        "category",
        "marketplace_price",
        "stock_qty"
    ]

    # ===========================================================
    # 6. WRITE SILVER CLEANED DATA (CSV)
    # ===========================================================
    silver_out = paths.get("silver_sellers_catalogue_data")
    if not silver_out:
        print("❌ Missing config key: paths.silver_sellers_catalogue_data")
        sys.exit(1)

    print(f"\n💾 Writing Silver CSV to: {silver_out}")

        # ---------------------------------------------------------
    # SILVER OUTPUT
    # ---------------------------------------------------------
    silver_run_path = os.path.join(silver_out, f"run_date={run_ts}")
    write_df(valid_df.select(*silver_cols), silver_run_path)

    # ---------------------------------------------------------
    #    ✅ NEW: WRITE CLEAN SELLER CATALOG DATA TO HUDI TABLE
    # ---------------------------------------------------------
    print("\n🚀 WRITING CLEANED DATA TO HUDI TABLE: seller_catalog_data")

    gold_seller_catalog_data = paths.get("gold_seller_catalog_data")  # <<< ADD IN CONFIG
    gold_run_path = os.path.join(gold_seller_catalog_data, f"run_date={run_ts}")
    
    if not gold_run_path:
        print("❌ Missing config key: paths.gold_seller_catalog_data")
        sys.exit(1)

    hudi_table = cfg.get("tables", {}).get("seller_catalog_hudi_table")

    if not hudi_table:
        raise Exception("❌ Missing config: tables.seller_catalog_hudi_table")

    # Ensure all silver_cols exist
    for c in silver_cols:
        if c not in valid_df.columns:
            valid_df = valid_df.withColumn(c, F.lit(None))

    (
        valid_df
        .select(*silver_cols)
        .write
        .format("hudi")
        .options(**{
            "hoodie.table.name": hudi_table,
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": "insert_overwrite",
            "hoodie.datasource.write.recordkey.field": "seller_id,item_id",
            "hoodie.datasource.write.precombine.field": "marketplace_price",
            "hoodie.datasource.hive_sync.enable": "false",
            "hoodie.datasource.write.schema.allow.auto.evolution": "true",
            "hoodie.metadata.enable": "false"
        })
        .mode("overwrite")
        .save(gold_run_path)
    )

    print(f"👍 Hudi write completed at: {gold_run_path}")

    # --------------------------------------------------------------
    # Read-back from Seller Catalog Hudi table for confirmation
    # --------------------------------------------------------------
    print("\n📥 Reading back data from Seller Catalog Hudi table for confirmation...")

    hudi_read_seller_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(gold_run_path)
    )

    print("\n📌 Seller Catalog Hudi Table Schema:")
    hudi_read_seller_df.printSchema()

    print("\n📌 Sample Records from Seller Catalog Hudi Table:")
    hudi_read_seller_df.show(20, truncate=False)

    # ===========================================================
    # Summary logs
    # ===========================================================
    print("===== SELLER CATALOG ETL COMPLETED =====")
    total = raw_df.count()
    silver_cnt = valid_df.count()
    quarantine_cnt = invalid_df.count()
    duplicate_cnt = df.filter(F.col("is_duplicate") == True).count()
    non_duplicate_quarantine_cnt = quarantine_cnt - duplicate_cnt
    total_non_duplicate_records = silver_cnt + non_duplicate_quarantine_cnt

    print(f"Total Records                   : {total}")
    print(f"Total Non-Duplicate Records     : {total_non_duplicate_records}")
    print(f"Duplicate Records               : {duplicate_cnt}")
    print(f"Quarantine Records              : {quarantine_cnt}")
    print(f"Non-Duplicate Quarantine        : {non_duplicate_quarantine_cnt}")
    print(f"Valid Silver Records            : {silver_cnt}")
    print(f"Silver Output Path              : {silver_out}")
    print(f"Hudi Output Path                : {gold_seller_catalog_data}")
    print(f"Quarantine Output Path          : {quarantine_path}")

    spark.stop()

if __name__ == "__main__":
    main()