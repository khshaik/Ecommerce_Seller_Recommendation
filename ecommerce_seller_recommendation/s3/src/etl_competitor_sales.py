#!/usr/bin/env python3
"""
etl_competitor_sales.py

===================================================
Competitor Sales ETL (Bronze -> Silver + Quarantine)
==================================================
- Reads raw competitor sales CSV from config
- Cleans & standardizes fields
- Parses dates using multiple formats
- Applies strict DQ rules on key business columns
- Handles duplicates strictly based on (seller_id + item_id) composite key
- Writes:
    - Silver → CSV
    - Quarantine → CSV, partitioned by run_date

===================================================
Data Validation Scenarios (Reference Table)
===================================================
    Columns: item_id, seller_id, units_sold, revenue, marketplace_price, sale_date
+---------+-----------+------------+----------------+--------------------------+-------------------------------+------------+
| item_id | seller_id | units_sold | revenue     | marketplace_price | sale_date  | reason                       | Is valid |
| ------- | --------- | ---------- | ----------- | ----------------- | ---------- | ---------------------------- | -------- |
| I50001  | C101      | 820        | 110129976.7 | 5420              | 27/10/25   | valid                        | yes      |
| I50002  | C102      | 909        | 119551162.9 | 7000              | 2025-13-45 | invalid date format          | no       |
| I50003  | C103      | 699        | 52531306.25 | (empty)           | 11/10/25   | missing marketplace_price    | no       |
| (empty) | C105      | 601        | 69096975.03 | 1200              | 19/09/25   | missing item_id              | no       |
| I50005  | (empty)   | 1498       | 103268794.2 | 2000              | 22/09/25   | missing seller_id            | no       |
| I50006  | C108      | (empty)    | 6301311.02  | 3400              | 22/09/25   | missing units_sold           | no       |
| I50007  | C109      | 562        | (empty)     | 3500              | 22/09/25   | missing revenue              | no       |
| I50009  | C111      | 1243       | 0           | 5000              | 21/09/25   | valid (zero revenue allowed) | yes      |
| I50011  | C113      | 0          | 1243        | 5000              | 21/09/25   | valid (zero units allowed)   | yes      |
| I50012  | C114      | 1972       | 249770406.2 | -1200             | 28/10/25   | price < 0                    | no       |
| I50015  | C117      | 951        | 134122802.6 | 2100              | 17/09/25   | duplicate seller_id+item_id  | no       |
| I50021  | C121      | -2000      | 58768340.52 | 1800              | 30/09/25   | units_sold < 0               | no       |
| I50021  | C121      | 1972       | -2000       | 1800              | 30/09/25   | revenue < 0                  | no       |
+---------+-----------+------------+----------------+--------------------------+-------------------------------+-------------+

===================================================
DQ Rules Applied
===================================================
- item_id must NOT be null/empty
- seller_id must NOT be null/empty
- units_sold must be numeric and >= 0
- revenue must be numeric and >= 0
- marketplace_price must be numeric and >= 0
- sale_date must parse successfully AND must be <= today
- Duplicates strictly based on (seller_id + item_id)
    - First valid occurrence → kept
    - Subsequent occurrences → Quarantine
- Zero values ARE allowed for:
        units_sold, revenue, marketplace_price

===================================================
Cleaning Steps
===================================================
• Trim and Normalize whitespace in (item_id, seller_id)
    - Normalize string casing if required
• Convert Data Types
    - units_sold → INT
    - revenue → DOUBLE
    - marketplace_price → DOUBLE
    - sale_date → DATE
• Fill Missing Numeric Fields
    - (Strict, but zero allowed)
    - units_sold → 0 if missing
    - revenue → 0 if missing
    - marketplace_price → 0 if missing
• Standardize Date Formats
    - Parse multiple formats: dd/MM/yy, dd/MM/yyyy, yyyy-MM-dd
        - If parsing fails → Quarantine
    - Remove future dates (sale_date > current_date())
• Remove Duplicates (seller_id + item_id)
• First valid record kept
    - Remaining → Quarantine with reason "duplicate key"

• Final Split
    Silver: Clean, valid, unique records
    Quarantine: Any row failing any rule
        - Missing item_id / seller_id
        - Negative numeric values
        - Invalid price
        - Invalid or future sale_date
        - Duplicate keys

===================================================
Bronze/Silver Files Records
===================================================
------------------------------------------------------------------------------------------
| Metric                       | Expected Behavior                       | Your Value    | 
| ---------------------------- | --------------------------------------- | ------------- | 
| Total Bronze                 | Raw input                               | 1,000,000     | 
| Non-duplicate                | Unique `item_id` after de-dupe          | 980,104       | 
| Duplicate rows               | Bronze − Non-duplicate                  | 19,896        | 
| Total Quarantine             | Failed DQ rules                         | 96,304        |      
| Non-duplicate Quarantine     | Unique rows failing DQ (not duplicates) | 76,408        | 
| Valid Silver Records         | Clean + unique, all DQ passed           | 903,696       |      
------------------------------------------------------------------------------------------

Supports Hudi output (overwrite mode)
* Quarantine data written to CSV file within Quanrantine folder. 
* Data post Clean + DQ steps written to CSV file within Silver folder.
* Cleaned valid data written to Hudi, following Medallion + Quarantine design
* Idempotent upsert-ready Hudi table (company_sales_data)
* Supports schema evolution (hoodie.datasource.write.schema.allow.auto.evolution)
* Supports overwrite mode (hoodie.datasource.write.operation=insert_overwrite)

# ================================================================
# Hudi Table Type Comparison
# ================================================================
# | Hudi Table Type         | Write Speed             | Read Speed | Files Written     | Best For                     |
# | ----------------------- | ----------------------- | ---------- | ------------------ | ---------------------------- |
# | COPY_ON_WRITE (COW)     | Slow (rewrites parquet) | Fast       | Parquet only       | BI queries, batch loads      |
# | MERGE_ON_READ (MOR)     | Fast                    | Medium     | Parquet + Log      | Streaming upserts, real-time |
# ================================================================

MERGE_ON_READ (MOR) instead of COPY_ON_WRITE (COW)?
* MERGE_ON_READ more flexible and ideal for ETL systems with frequent updates or incremental schema evolution, due to old Parquet files + new log files which can coexist even if schema changes.
* Uses insert_overwrite (clean overwrite) operation, which is schema-evolution friendly as it replaces previous version while letting Hudi evolve schema forward.
* Hudi stores schema in metadata and compares incoming schema with stored schema and evolves, if allowed.

"""

import sys
import os
import yaml
from datetime import datetime
from typing import List

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window

# ---------------------------
# YAML loader
# ---------------------------
def load_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------------------------
# Spark session builder
# ---------------------------
def get_spark(app_name: str, shuffle_partitions: int = 8):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    # help with legacy two-digit year parsing
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    return spark


# ---------------------------
# write csv util (local)
# ---------------------------
def write_csv(df, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)


# ---------------------------
# multi-format date parser
# ---------------------------
def parse_date_multi(col_name: str, fmts: List[str]):
    """
    Try multiple formats; also handle dd/MM/yy -> dd/MM/20yy
    Returns a Column with to_date(...) attempts coalesced.
    """
    expr = None
    for f in fmts:
        d = F.to_date(F.col(col_name), f)
        expr = d if expr is None else F.coalesce(expr, d)

    # fallback attempts
    expr = F.coalesce(
        expr,
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "dd-MM-yyyy"),
        F.to_date(F.col(col_name), "yyyy-MM-dd")
    )

    # normalize two-digit year: dd/MM/yy -> dd/MM/20yy
    expr = F.coalesce(
        expr,
        F.when(
            F.col(col_name).rlike(r"^\d{1,2}/\d{1,2}/\d{2}$"),
            F.to_date(
                F.concat(
                    F.substring_index(F.col(col_name), "/", 1),
                    F.lit("/"),
                    F.substring_index(F.col(col_name), "/", -2).substr(1, 2),
                    F.lit("/20"),
                    F.substring_index(F.col(col_name), "/", -1)
                ),
                "d/M/yyyy"
            )
        )
    )

    return expr

def show_quarantine_preview(df, key_cols=None, max_rows=30, truncate=60):
    """
    Clean, compact quarantine preview for logs.
    Shows only important columns + grouped by number of DQ issues.
    """
    if key_cols is None:
        key_cols = [
            "raw_seller_id",
            "raw_item_id",
            "raw_units_sold",
            "raw_revenue",
            "raw_marketplace_price",
            "raw_sale_date",
            "dq_failure_reason"
        ]

    # Take only columns that exist
    cols = [c for c in key_cols if c in df.columns]

    if not cols:
        cols = df.columns[:10]  # fallback small subset

    # Group rows with more DQ issues first
    preview = df.select(*cols).orderBy(
        F.size(F.split(F.col("dq_failure_reason"), ": ")).desc()
    )
    
    preview.show(max_rows, truncate=truncate)

# ---------------------------
# MAIN
# ---------------------------
run_ts = datetime.today().strftime("%Y-%m-%d_%H%M%S")

def main():
    if "--config" not in sys.argv:
        print("Usage: etl_competitor_sales.py --config <config.yml>")
        sys.exit(1)

    cfg_path = sys.argv[sys.argv.index("--config") + 1]
    if not os.path.exists(cfg_path):
        print(f"❌ Config not found: {cfg_path}")
        sys.exit(1)

    cfg = load_yaml(cfg_path)
    paths = cfg.get("paths", {})
    dq_cfg = cfg.get("dq", {})
    spark_cfg = cfg.get("spark", {}).get("shuffle_partitions", 8)

    raw_path = paths.get("raw_competitor_sales_data")
    silver_path = paths.get("silver_competitor_sales_data")
    quarantine_root = paths.get("quarantine_competitor_sales_data")
    bronze_root = paths.get("bronze_root")

    if not raw_path or not silver_path or not quarantine_root:
        print("❌ Required paths missing in config → (raw_competitor_sales_data / silver_competitor_sales_data / quarantine_competitor_sales_data)")
        sys.exit(1)

    spark = get_spark("CompetitorSalesETL", shuffle_partitions=spark_cfg)

    # ---------------------------
    # READ RAW
    # ---------------------------
    print("\n========== ** INPUT: RAW COMPETITOR SALES DATA ** ==========")
    raw_df = spark.read.option("header", True).csv(raw_path)
    raw_df.show(20, truncate=False)

    # optional bronze backup
    if bronze_root:
        bronze_out = os.path.join(bronze_root, f"competitor_sales/run_date={run_ts}")
        write_csv(raw_df, bronze_out)

    # ensure expected raw columns exist as strings
    expected_cols = ["seller_id", "item_id", "units_sold", "revenue", "marketplace_price", "sale_date"]
    for c in expected_cols:
        if c not in raw_df.columns:
            raw_df = raw_df.withColumn(c, F.lit(None).cast(StringType()))

    # keep raw trimmed strings for required-field checks (do NOT cast yet)
    df = raw_df.withColumn("raw_seller_id", F.trim(F.col("seller_id"))) \
               .withColumn("raw_item_id", F.trim(F.col("item_id"))) \
               .withColumn("raw_units_sold", F.trim(F.col("units_sold"))) \
               .withColumn("raw_revenue", F.trim(F.col("revenue"))) \
               .withColumn("raw_marketplace_price", F.trim(F.col("marketplace_price"))) \
               .withColumn("raw_sale_date", F.trim(F.col("sale_date")))

    print("\n>>> RAW (trimmed) preview:")
    df.select("raw_seller_id", "raw_item_id", "raw_units_sold", "raw_revenue", "raw_marketplace_price", "raw_sale_date").show(50, truncate=False)

    # ---------------------------
    # Parse / normalize date
    # ---------------------------
    formats = dq_cfg.get("competitor_date_formats", dq_cfg.get("company_sales_date_checks", {}).get("allowed_formats", ["dd/MM/yyyy","dd/MM/yy","yyyy-MM-dd","dd-MM-yyyy"]))
    # normalize dd/MM/yy -> dd/MM/20yy in sale_date_norm
    df = df.withColumn(
        "sale_date_norm",
        F.when(
            (F.col("raw_sale_date").isNotNull()) & F.col("raw_sale_date").rlike(r"^\d{1,2}/\d{1,2}/\d{2}$"),
            F.concat(
                F.substring_index(F.col("raw_sale_date"), "/", 1), F.lit("/"),
                F.substring_index(F.col("raw_sale_date"), "/", -2).substr(1, 2), F.lit("/20"),
                F.substring_index(F.col("raw_sale_date"), "/", -1)
            )
        ).otherwise(F.col("raw_sale_date"))
    )

    df = df.withColumn("sale_date_parsed", parse_date_multi("sale_date_norm", formats))
    df = df.withColumn("sale_date_final", F.col("sale_date_parsed"))
    df = df.withColumn("sale_date_invalid", F.col("sale_date_parsed").isNull())
    df = df.withColumn("sale_date_future", F.col("sale_date_final") > F.current_date())

    # ---------------------------
    # Safe numeric parsing (operate on raw strings)
    # ---------------------------
    df = df.withColumn(
        "units_sold_num",
        F.when(F.col("raw_units_sold").rlike(r"^-?\d+(\.\d+)?$"), F.col("raw_units_sold").cast(DoubleType()))
         .otherwise(None)
    )

    df = df.withColumn(
        "revenue_num",
        F.when(F.col("raw_revenue").rlike(r"^-?\d+(\.\d+)?$"), F.col("raw_revenue").cast(DoubleType()))
         .otherwise(None)
    )

    df = df.withColumn(
        "marketplace_price_num",
        F.when(F.col("raw_marketplace_price").rlike(r"^-?\d+(\.\d+)?$"), F.col("raw_marketplace_price").cast(DoubleType()))
         .otherwise(None)
    )

    # ---------------------------
    # DQ flags (required-field checks use raw_*)
    # ---------------------------
    df = df.withColumn("missing_seller_id", F.col("raw_seller_id").isNull() | (F.col("raw_seller_id") == "")) \
           .withColumn("missing_item_id", F.col("raw_item_id").isNull() | (F.col("raw_item_id") == "")) \
           .withColumn("missing_units_sold", F.col("raw_units_sold").isNull() | (F.col("raw_units_sold") == "")) \
           .withColumn("missing_revenue", F.col("raw_revenue").isNull() | (F.col("raw_revenue") == "")) \
           .withColumn("missing_marketplace_price", F.col("raw_marketplace_price").isNull() | (F.col("raw_marketplace_price") == "")) \
           .withColumn("missing_sale_date", F.col("raw_sale_date").isNull() | (F.col("raw_sale_date") == ""))

    # numeric domain checks (only when parsed numeric exists)
    num_checks = dq_cfg.get("competitor_sales_numeric_checks", {"units_sold_min": 0, "revenue_min": 0, "marketplace_price_min": 0})
    df = df.withColumn("units_sold_negative", F.when(F.col("units_sold_num").isNotNull() & (F.col("units_sold_num") < num_checks.get("units_sold_min", 0)), True).otherwise(False))
    df = df.withColumn("revenue_negative", F.when(F.col("revenue_num").isNotNull() & (F.col("revenue_num") < num_checks.get("revenue_min", 0)), True).otherwise(False))
    df = df.withColumn("marketplace_price_negative", F.when(F.col("marketplace_price_num").isNotNull() & (F.col("marketplace_price_num") < num_checks.get("marketplace_price_min", 0)), True).otherwise(False))

    # raw malformed date heuristic
    df = df.withColumn(
    "raw_date_malformed",
    ~F.col("raw_sale_date").rlike(
        r"^((\d{1,2}/\d{1,2}/\d{2})|(\d{1,2}/\d{1,2}/\d{4})|(\d{4}-\d{2}-\d{2}))$"))

    # ---------------------------
    # Build dq_failure_reasons array (strict Option B)
    # Use expr("filter(..., x -> x is not null)") to drop nulls reliably
    # ---------------------------
    df = df.withColumn(
        "dq_failure_reasons",
        F.expr("""
        filter(
            array(
                CASE WHEN missing_seller_id THEN 'seller_id_missing' END,
                CASE WHEN missing_item_id THEN 'item_id_missing' END,
                CASE WHEN missing_units_sold THEN 'units_sold_missing' END,
                CASE WHEN missing_revenue THEN 'revenue_missing' END,
                CASE WHEN missing_marketplace_price THEN 'marketplace_price_missing' END,
                CASE WHEN missing_sale_date THEN 'sale_date_missing' END,
                CASE WHEN raw_date_malformed THEN 'unparseable_sale_date' END,
                CASE WHEN sale_date_invalid THEN 'malformed_sale_date' END,
                CASE WHEN sale_date_future THEN 'future_sale_date' END,
                CASE WHEN units_sold_negative THEN 'units_sold_negative' END,
                CASE WHEN revenue_negative THEN 'revenue_negative' END,
                CASE WHEN marketplace_price_negative THEN 'marketplace_price_negative' END
            ),
            x -> x IS NOT NULL
            )
        """)
    )

    # ---------------------------
    # Duplicate detection (seller_id + item_id)
    # ---------------------------
    dup_win = Window.partitionBy("raw_seller_id", "raw_item_id").orderBy(F.monotonically_increasing_id())
    df = df.withColumn("rn", F.row_number().over(dup_win))

    df = df.withColumn(
        "dq_failure_reasons",
        F.when(
            (F.col("raw_item_id").isNotNull()) & (F.col("raw_seller_id").isNotNull()) & (F.col("rn") > 1),
            F.array_union(F.col("dq_failure_reasons"), F.array(F.lit("duplicate_seller_item")))
        ).otherwise(F.col("dq_failure_reasons"))
    )

    # final dq_pass (zero failures)
    df = df.withColumn("dq_pass", F.size(F.col("dq_failure_reasons")) == 0)

    # ---------------------------
    # Split passed/failed — only cast typed columns for passed rows
    # ---------------------------
    passed_df = df.filter(F.col("dq_pass") == True)
    failed_df = df.filter(F.col("dq_pass") == False)

    # cast typed columns on passed rows (safe)
    passed_df = passed_df.withColumn("seller_id", F.col("raw_seller_id")) \
                         .withColumn("item_id", F.col("raw_item_id")) \
                         .withColumn("units_sold", F.col("units_sold_num").cast(DoubleType())) \
                         .withColumn("revenue", F.col("revenue_num").cast(DoubleType())) \
                         .withColumn("marketplace_price", F.col("marketplace_price_num").cast(DoubleType())) \
                         .withColumn("sale_date_final", F.col("sale_date_final"))

    # flags indicating filled defaults (none for Option B)
    passed_df = passed_df.withColumn("units_sold_filled", F.lit(False)) \
                         .withColumn("revenue_filled", F.lit(False)) \
                         .withColumn("marketplace_price_filled", F.lit(False))

    # union pass+fail back together (so counts/diffs are easy) — harmonize columns
    common_cols = list(set(passed_df.columns).union(set(failed_df.columns)))
    for c in common_cols:
        if c not in passed_df.columns:
            passed_df = passed_df.withColumn(c, F.lit(None))
        if c not in failed_df.columns:
            failed_df = failed_df.withColumn(c, F.lit(None))

    df = passed_df.select(*common_cols).unionByName(failed_df.select(*common_cols))

    # recompute dq_pass after the union (unchanged logically)
    df = df.withColumn("dq_pass", F.size(F.col("dq_failure_reasons")) == 0)

    # ---------------------------
    # Final split and prepare quarantine reason string
    # ---------------------------
    valid_df = df.filter(F.col("dq_pass") & (F.col("rn") == 1))
    invalid_df = df.filter((~F.col("dq_pass")) | (F.col("rn") > 1))
    invalid_df = invalid_df.withColumn("dq_failure_reason", F.concat_ws(": ", F.col("dq_failure_reasons")))

    # ---------------------------
    # Write Quarantine
    # ---------------------------
    print("\n❌ QUARANTINE ROWS (Invalid Records with DQ Reasons):")
    quarantine_path = os.path.join(quarantine_root, f"run_date={run_ts}")

    #invalid_df.show(50, truncate=False)
    show_quarantine_preview(
    invalid_df,
    key_cols=[
        "raw_seller_id",
        "raw_item_id",
        "raw_units_sold",
        "raw_revenue",
        "raw_marketplace_price",
        "raw_sale_date",
        "dq_failure_reason"
    ],
    max_rows=50,
    truncate=60
)
    write_csv(invalid_df.drop("dq_failure_reasons"), quarantine_path)

    # ===========================================================
    # 6. WRITE SILVER CLEANED DATA (CSV)
    # ===========================================================
    print("\n✅ CLEAN SILVER DATA (Valid Records After DQ + Dedupe):")
    silver_cols = [
        "seller_id",
        "item_id",
        "units_sold",
        "revenue",
        "marketplace_price",
        "sale_date_final",
        "units_sold_filled",
        "revenue_filled",
        "marketplace_price_filled"
    ]
    for c in silver_cols:
        if c not in valid_df.columns:
            valid_df = valid_df.withColumn(c, F.lit(None))

    valid_df.select(*silver_cols).show(50, truncate=False)
    # ---------------------------------------------------------
    # SILVER OUTPUT
    # ---------------------------------------------------------
    silver_run_path = os.path.join(silver_path, f"run_date={run_ts}")
    write_csv(valid_df.select(*silver_cols), silver_run_path)

   # ---------------------------------------------------------
    #    ✅ NEW: WRITE CLEAN SELLER CATALOG DATA TO HUDI TABLE
    # ---------------------------------------------------------
    print("\n🚀 WRITING CLEANED DATA TO HUDI TABLE: competitor_sales_data")

    gold_competitor_sales_data = paths.get("gold_competitor_sales_data")
    gold_run_path = os.path.join(gold_competitor_sales_data, f"run_date={run_ts}")
    
    if not gold_run_path:
        print("❌ Missing config key: paths.gold_competitor_sales_data")
        sys.exit(1)

    hudi_table = cfg.get("tables", {}).get("competitor_sales_hudi_table")

    if not hudi_table:
        raise Exception("❌ Missing config: tables.competitor_sales_hudi_table")

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
            "hoodie.datasource.write.precombine.field": "sale_date_final",
            "hoodie.datasource.hive_sync.enable": "false",
            "hoodie.datasource.write.schema.allow.auto.evolution": "true",
            "hoodie.metadata.enable": "false"
        })
        .mode("overwrite")
        .save(gold_run_path)
    )

    print(f"👍 Hudi write completed at: {gold_run_path}")

    # --------------------------------------------------------------
    # Read-back from Competitor Sales Hudi table for confirmation
    # --------------------------------------------------------------
    print("\n📥 Reading back data from Competitor Sales Hudi table for confirmation...")

    hudi_read_comp_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(gold_run_path)
    )

    print("\n📌 Competitor Sales Hudi Table Schema:")
    hudi_read_comp_df.printSchema()

    print("\n📌 Sample Records from Competitor Sales Hudi Table:")
    hudi_read_comp_df.show(20, truncate=False)

    # ---------------------------
    # Summary
    # ---------------------------
    print("\n===== COMPETITOR SALES ETL COMPLETED =====")
    total = raw_df.count()
    silver_cnt = valid_df.count()
    quarantine_cnt = invalid_df.count()

    duplicate_cnt = df.filter(F.col("rn") > 1).count()
    non_duplicate_quarantine_cnt = quarantine_cnt - duplicate_cnt
    total_non_duplicate_records = silver_cnt + non_duplicate_quarantine_cnt

    print(f"Total Records                   : {total}")
    print(f"Total Non-Duplicate Records     : {total_non_duplicate_records}")
    print(f"Duplicate Records               : {duplicate_cnt}")
    print(f"Quarantine Records              : {quarantine_cnt}")
    print(f"Non-Duplicate Quarantine        : {non_duplicate_quarantine_cnt}")
    print(f"Valid Silver Records            : {silver_cnt}")
    print(f"Silver Output Path              : {silver_path}")
    print(f"Hudi Output Path                : {gold_competitor_sales_data}")
    print(f"Quarantine Output Path          : {quarantine_path}")

    spark.stop()

if __name__ == "__main__":
    main()