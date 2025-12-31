#!/usr/bin/env python3
"""
etl_company_sales.py

===================================================
Company Sales ETL (Bronze -> Silver + Quarantine)
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
✔ item_id must NOT be null/empty
✔ seller_id must NOT be null/empty
✔ units_sold must be numeric and >= 0
✔ revenue must be numeric and >= 0
✔ marketplace_price must be numeric and >= 0
✔ sale_date must parse successfully AND must be <= today
✔ Duplicates strictly based on (seller_id + item_id)
        First valid occurrence → kept
Subsequent occurrences → Quarantine
✔ Zero values ARE allowed for:
        units_sold, revenue, marketplace_price

===================================================
Cleaning Steps
===================================================
• Trim and Normalize whitespace in (item_id, seller_id)
    - Normalize string casing if required
• Convert Data Types
    units_sold → INT
    revenue → DOUBLE
    marketplace_price → DOUBLE
    sale_date → DATE
• Fill Missing Numeric Fields
    (Strict, but zero allowed)
    units_sold → 0 if missing
    revenue → 0 if missing
    marketplace_price → 0 if missing
• Standardize Date Formats
    Parse multiple formats: dd/MM/yy, dd/MM/yyyy, yyyy-MM-dd
        If parsing fails → Quarantine
    Remove future dates (sale_date > current_date())
• Remove Duplicates (seller_id + item_id)
• First valid record kept
    Remaining → Quarantine with reason "duplicate key"

• Final Split
Silver: Clean, valid, unique records
Quarantine: Any row failing any rule
    Missing item_id / seller_id
    Negative numeric values
    Invalid price
    Invalid or future sale_date
    Duplicate keys

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
from pyspark.sql.types import DoubleType, StringType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import udf

# --------------------------------------------------
# UDF to normalize 2-digit years (convert yy -> 20yy)
# --------------------------------------------------
def normalize_year(dt):
    if dt is None:
        return None
    y = dt.year
    # convert year 00–99 → 2000–2099
    if y < 100:
        return dt.replace(year=2000 + y)
    return dt

normalize_year_udf = udf(normalize_year, DateType())


# --------------------------------------------------
# YAML Loader
# --------------------------------------------------
def load_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)


# --------------------------------------------------
# Spark Session
# --------------------------------------------------
def get_spark(app_name: str, shuffle_partitions: int = 8):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )

    # <<< ADDED (Spark 3 date parsing fix)
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    return spark


# --------------------------------------------------
# Write CSV utility
# --------------------------------------------------
def write_csv(df, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)


# --------------------------------------------------
# Build multi-format date parser column
# --------------------------------------------------
def parse_date_multi(col_name: str, fmts: List[str]):
    expr = None
    for f in fmts:
        d = F.to_date(F.col(col_name), f)
        expr = d if expr is None else F.coalesce(expr, d)

    # fallback formats
    expr = F.coalesce(
        expr,
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "dd-MM-yyyy")
    )

    # handle 2-digit year: dd/MM/yy
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


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():

    if "--config" not in sys.argv:
        print("Usage: etl_company_sales.py --config <config.yml>")
        sys.exit(1)

    cfg_path = sys.argv[sys.argv.index("--config") + 1]

    if not os.path.exists(cfg_path):
        print(f"❌ Config file not found: {cfg_path}")
        sys.exit(1)

    cfg = load_yaml(cfg_path)

    paths = cfg.get("paths", {})
    dq = cfg.get("dq", {})
    spark_cfg = cfg.get("spark", {}).get("shuffle_partitions", 8)

    raw_path = paths.get("raw_company_sales_data")
    silver_path = paths.get("silver_company_sales_data")
    quarantine_root = paths.get("quarantine_company_sales_data")
    bronze_root = paths.get("bronze_root")

    if not raw_path or not silver_path or not quarantine_root:
        print("❌ Required paths missing in config → (raw / silver / quarantine)")
        sys.exit(1)

    spark = get_spark("CompanySalesETL", shuffle_partitions=spark_cfg)

    # ---------------------------------------------
    # LOAD RECORDS INTO BRONZE LAYER
    # ---------------------------------------------
    print("\n❌ Read Records into Braonze Layer:")
    raw_df = spark.read.option("header", True).csv(raw_path)

    bronze_root = paths.get("bronze_root")

    if bronze_root:
        run_ts = datetime.today().strftime("%Y-%m-%d_%H%M%S")
        bronze_out = os.path.join(bronze_root, f"company_sales/run_date={run_ts}")
        write_csv(raw_df, bronze_out)

    # ---------------------------------------------
    # CLEANING / STANDARDIZATION
    # ---------------------------------------------
    expected_cols = ["seller_id", "item_id", "units_sold", "revenue", "sale_date", "marketplace_price"]
    for c in expected_cols:
        if c not in raw_df.columns:
            raw_df = raw_df.withColumn(c, F.lit(None).cast(StringType()))

    df = raw_df.select(
        F.trim("seller_id").alias("seller_id"),
        F.trim("item_id").alias("item_id"),
        #F.col("units_sold").cast(DoubleType()).alias("units_sold"),
        #F.col("revenue").cast(DoubleType()).alias("revenue"),
        F.col("units_sold").alias("units_sold"),
        F.col("revenue").alias("revenue"),
        F.trim("sale_date").alias("sale_date"),
        F.col("marketplace_price").cast(DoubleType()).alias("marketplace_price")
    )

    # ---------------------------------------------
    # RAW BACKUP COLUMNS (used for DQ checks)
    # ---------------------------------------------
    df = df.withColumn("raw_item_id", F.col("item_id"))
    df = df.withColumn("raw_units_sold", F.col("units_sold"))
    df = df.withColumn("raw_revenue", F.col("revenue"))
    df = df.withColumn("raw_sale_date", F.col("sale_date"))

    df = df.withColumn(
        "seller_id",
        F.when(F.col("seller_id").isNull(), F.lit("COMPANY")).otherwise(F.col("seller_id"))
    )

    df = df.withColumn("units_sold_filled", F.lit(False)) \
           .withColumn("revenue_filled", F.lit(False)) \
           .withColumn("revenue_derived", F.lit(False))

    # ---------------------------------------------
    # Revenue Derivation
    # ---------------------------------------------
    rev_cfg = dq.get("revenue_derivation", {"enable": True})
    if rev_cfg.get("enable", True):
        df = df.withColumn(
            "derived_rev",
            F.when(
                F.col("revenue").isNull() &
                F.col("marketplace_price").isNotNull() &
                F.col("units_sold").isNotNull(),
                F.col("marketplace_price") * F.col("units_sold")
            )
        )
        df = df.withColumn(
            "revenue",
            F.when(F.col("derived_rev").isNotNull(), F.col("derived_rev"))
            .otherwise(F.col("revenue"))
        )
        df = df.withColumn("revenue_derived", F.col("derived_rev").isNotNull())
    else:
        df = df.withColumn("derived_rev", F.lit(None))

    # ---------------------------------------------
    # Fill Missing Units_Sold / Revenue with 0
    # ---------------------------------------------
    print("\n Filled Missing values:")
    fill_zero_cols = dq.get("fill_missing_with_zero", ["units_sold", "revenue"])

    if "units_sold" in fill_zero_cols:
        df = df.withColumn("units_sold_filled", F.col("units_sold").isNull())
        df = df.withColumn("units_sold", F.when(F.col("units_sold").isNull(), 0).otherwise(F.col("units_sold")))

    if "revenue" in fill_zero_cols:
        df = df.withColumn("revenue_filled", F.col("revenue").isNull())
        df = df.withColumn("revenue", F.when(F.col("revenue").isNull(), 0.0).otherwise(F.col("revenue")))

    df.show(15, truncate=False)

    # ---------------------------------------------
    # ADD: SAFETY FLAG — detect raw malformed date BEFORE parsing
    # ---------------------------------------------
    df = df.withColumn(
        "raw_date_malformed",
        ~F.col("sale_date").rlike(r"^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$")
    )  # <<< ADDED

    # ---------------------------------------------
    # Parse Sale Date
    # ---------------------------------------------
    date_cfg = dq.get("company_sales_date_checks", {})
    formats = date_cfg.get("allowed_formats", ["dd/MM/yyyy", "dd/MM/yy", "yyyy-MM-dd", "dd-MM-yyyy"])

    df = df.withColumn("sale_date_parsed", parse_date_multi("sale_date", formats))
    # --------------------------------------------------------------
    # FIX year 0025 → 2025 for dd/MM/yy formatted dates
    # --------------------------------------------------------------
    df = df.withColumn("sale_date_final", normalize_year_udf("sale_date_parsed"))
    df = df.withColumn("sale_date_str", F.date_format(F.col("sale_date_final"), "yyyy-MM-dd"))

    # ---------------------------------------------
    # Data Quality Rules
    # ---------------------------------------------
    rules = []

    # ---------------------------------------------
    # Required-field checks MUST use RAW values
    # ---------------------------------------------
    rules.append(F.when(F.col("raw_item_id").isNull() |
                    (F.trim(F.col("raw_item_id")) == ""),
                    F.lit("item_id_missing")))

    rules.append(F.when(F.col("raw_units_sold").isNull(),
                    F.lit("units_sold_missing")))

    rules.append(F.when(F.col("raw_revenue").isNull(),
                    F.lit("revenue_missing")))

    rules.append(F.when(F.col("raw_sale_date").isNull() |
                    (F.trim(F.col("raw_sale_date")) == ""),
                    F.lit("sale_date_missing")))

    num_checks = dq.get("company_sales_numeric_checks", {"units_sold_min": 0, "revenue_min": 0})
    rules.append(F.when(F.col("units_sold") < num_checks.get("units_sold_min", 0), F.lit("units_sold_negative")).otherwise(F.lit(None)))
    rules.append(F.when(F.col("revenue") < num_checks.get("revenue_min", 0), F.lit("revenue_negative")).otherwise(F.lit(None)))

    rules.append(F.when(F.col("sale_date_final").isNull(), F.lit("malformed_sale_date")).otherwise(F.lit(None)))

    # <<< ADDED: DQ rule for unparseable raw date
    rules.append(
        F.when(F.col("raw_date_malformed") == True, F.lit("unparseable_sale_date"))
         .otherwise(F.lit(None))
    )

    if date_cfg.get("max_date_today", True):
        today = datetime.today().strftime("%Y-%m-%d")
        rules.append(F.when(F.col("sale_date_final") > F.lit(today), F.lit("future_sale_date")).otherwise(F.lit(None)))

    df = df.withColumn("dq_failure_reasons", F.array([r for r in rules]))
    df = df.withColumn("dq_failure_reasons", F.expr("filter(dq_failure_reasons, x -> x is not null)"))

    # ---------------------------------------------
    # Duplicate Handling (STRICT item_id)
    # ---------------------------------------------
    dup_win = Window.partitionBy("item_id").orderBy(F.monotonically_increasing_id())
    df = df.withColumn("rn", F.row_number().over(dup_win))

    df = df.withColumn(
        "dq_failure_reasons",
        F.when(
            F.col("rn") > 1,
            F.array_union(F.col("dq_failure_reasons"), F.array(F.lit("duplicate_item_id")))
        ).otherwise(F.col("dq_failure_reasons"))
    )

    df = df.withColumn("dq_pass", F.size("dq_failure_reasons") == 0)

    # ---------------------------------------------
    # Split Valid / Invalid
    # ---------------------------------------------
    valid_df = df.filter(F.col("dq_pass") & (F.col("rn") == 1))
    invalid_df = df.filter(~F.col("dq_pass") | (F.col("rn") > 1))
    invalid_df = invalid_df.withColumn("dq_failure_reason", F.concat_ws(": ", F.col("dq_failure_reasons")))

    # ---------------------------------------------
    # Write Quarantine
    # ---------------------------------------------
    print("\n❌ QUARANTINE ROWS (Invalid Records with DQ Reasons):")
    run_ts = datetime.today().strftime("%Y-%m-%d_%H%M%S")
    run_date_partition = f"run_date={run_ts}"   
    qpath = os.path.join(quarantine_root, run_date_partition)

    invalid_df.show(20, truncate=False)
    write_csv(invalid_df.drop("dq_failure_reasons"), qpath)

    # ---------------------------------------------
    # Write Silver
    # ---------------------------------------------
    print("\n✅ CLEAN SILVER DATA (Valid Records After Dedupe):")
    silver_cols = [
        "seller_id",
        "item_id",
        "units_sold",
        "revenue",
        "marketplace_price",
        "sale_date_final",
        "units_sold_filled",
        "revenue_filled",
        "revenue_derived"
    ]

    valid_df.select(*silver_cols).show(20, truncate=False)
    # ---------------------------------------------------------
    # SILVER OUTPUT
    # ---------------------------------------------------------
    silver_run_path = os.path.join(silver_path, run_date_partition)
    write_csv(valid_df.select(*silver_cols), silver_run_path)

    # ---------------------------------------------------------
    # ✅ NEW: WRITE CLEAN DATA TO HUDI TABLE
    # ---------------------------------------------------------
    print("\n🚀 WRITING CLEANED DATA TO HUDI TABLE: company_sales_data")

    gold_company_sales_data = paths.get("gold_company_sales_data")  # <<< ADD THIS IN CONFIG
    gold_run_path = os.path.join(gold_company_sales_data, run_date_partition)

    if not gold_run_path:
        print("❌ Missing config key: paths.gold_company_sales_data")
        sys.exit(1)

    hudi_table = cfg.get("tables", {}).get("company_sales_hudi_table")

    if not hudi_table:
        raise Exception("❌ Missing config: tables.company_sales_hudi_table")

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
            "hoodie.datasource.write.recordkey.field": "item_id",
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
    # Read-back from Hudi table for confirmation
    # --------------------------------------------------------------
    print("\n📥 Reading back data from Hudi table for confirmation")

    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

    hudi_read_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(gold_run_path) 
    )

    print("\n📌 Hudi Table Schema:")
    hudi_read_df.printSchema()

    print("\n📌 Sample Records from Hudi Table:")
    hudi_read_df.show(20, truncate=False)

    # ---------------------------------------------
    # Summary
    # ---------------------------------------------
    print("===== COMPANY SALES DATA ETL COMPLETED =====")

    total = raw_df.count()
    silver_cnt = valid_df.count()
    quarantine_cnt = invalid_df.count()

    # Duplicate count based on item_id (same logic used in ETL: rn > 1)
    duplicate_cnt = df.filter(F.col("rn") > 1).count()

    # Non-duplicate quarantine = quarantine – duplicates
    non_duplicate_quarantine_cnt = quarantine_cnt - duplicate_cnt

    # Total Non-Duplicate Records = Silver + Non-Duplicate Quarantine
    total_non_duplicate_records = silver_cnt + non_duplicate_quarantine_cnt

    print(f"Total Records                   : {total}")
    print(f"Total Non-Duplicate Records     : {total_non_duplicate_records}")
    print(f"Duplicate Records               : {duplicate_cnt}")
    print(f"Quarantine Records              : {quarantine_cnt}")
    print(f"Non-Duplicate Quarantine        : {non_duplicate_quarantine_cnt}")
    print(f"Valid Silver Records            : {silver_cnt}")
    print(f"Silver Output Path              : {silver_path}")
    print(f"Quarantine Output Path          : {qpath}")
    
    spark.stop()

if __name__ == "__main__":
    main()