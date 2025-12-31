#!/usr/bin/env python3
"""
consumption_recommendation.py

This script generates item recommendations for sellers based on both company and competitor sales data. The recommendations are aimed at identifying missing top-selling items and estimating their potential revenue for sellers to add to their catalog.

============================
Company-Specific Objectives:
============================
1. Identify top-selling items within the company:
   - Analyze all sellers’ sales data to determine top-selling items by category.

2. Compare seller catalogs against top-selling items:**
   - Identify missing items in each seller’s catalog from the company’s top sellers.

3. **Compute business metrics for recommendations:
   - Calculate current price from company data.
   - Estimate expected revenue if the seller adds the item:
     - `expected_revenue = expected_units_sold * marketplace_price`

==============================
Competitor-Specific Objectives:
==============================
1. Analyze competitor data:
   - Identify top-selling items in the market based on competitor sales.

2. Recommend items missing from the company’s catalog:
   - Identify high-performing items in the competitor's catalog that are missing from the company catalog.

3. Compute business metrics for recommendations:
   - Calculate current price from competitor data.
   - Estimate expected revenue if the seller adds the item:
     - `expected_revenue = expected_units_sold * marketplace_price`

Execution Flow (Technical)
1. Load Hudi datasets (company, competitor, seller catalog)
2. Normalize item IDs, prepare item metadata
3. Compute top-N items per category (company & competitor)
4. Expand seller × top-item combinations via cross join
5. Remove items already sold (left-anti join)
6. Calculate expected units + expected revenue
7. Write global outputs + per-seller outputs
8. Log metrics and execution summary

===========================
Data Transformations & Calculations:
===========================
- **Reading Data from Hudi Tables**: 
   - The script reads sales and catalog data from **Hudi tables** stored in a data lake. These tables contain transactional data for both company and competitor sales.
   
- Aggregate **company** sales data to find top-selling items per category.
- Aggregate **competitor** sales data to identify market-leading items.
- Compare each seller’s catalog with top-selling items from both **company** and **competitor** catalogs.
  
- For missing items, compute:
  - `market_price` from either company or competitor data.
  - `expected_units_sold` based on historical sales:
    - `expected_units_sold = total_units_sold / number_of_sellers_selling_this_item`
  - `expected_revenue = expected_units_sold * market_price`

Generates seller recommendations for COMPANY and COMPETITOR top items.
      Reads Hudi tables (paths in config):
            - paths.gold_company_sales_data       (company_sales_data)
            - paths.gold_competitor_sales_data    (competitor_sales_data)
            - paths.gold_seller_catalog_data      (seller_catalog_data)

      Writes:
            - paths.gold_recommendation_company_data   (CSV, overwrite)
            - paths.gold_recommendation_competitor_data (CSV, overwrite)

===========================
Output File Format:
===========================
The script generates a recommendation file for each seller with the following columns:
      seller_id, item_id, item_name, category, market_price, expected_units_sold, expected_revenue

# Company Sales, Competitor Sales, Seller Catalog Overview
# Attribute                          | Company Sales  | Competitor Sales  | Seller Catalog
# ----------------------------------- | -------------- | ----------------- | ---------------
# Total Records                       | 1,000,000      | 1,000,000         | 1,000,500
# Quarantine Records                  | 96,304         | 58,461            | 78,001
# Valid Silver Records                | 903,696        | 941,539           | 922,499

Interpretation:
    The recommendation engine constructs full space of all possible (seller, top-selling item) combinations, a theoretical
    maximum of 2,040 recommendation pairs (51 sellers × 4 categories × 10 items). However, as the business requirement is to 
    recommend only those items which a seller does *not* currently stock, algorithm applies left-anti join to exclude seller–item 
    pairs already present in the seller catalog as part of which the filtering step removed 40 such combinations.

    The final dataset contains 2,000 valid recommendations, each representing a potential catalog expansion opportunity for seller i.e. items 
    with strong market demand that sellers are currently missing from their catalog. The distribution is consistent across categories (500 rows 
    per category) and retains intended 10 unique top items per category. Sellers receive an average of ~39 recommendations each, with variation 
    caused  exclusively by the removal of already-stocked items.

-------------------------------------------------------------------------
|                           Dimension               |   Value / Notes   |
-------------------------------------------------------------------------
| Total sellers                                     | 51                |
| Total product categories                          | 4                 |
  Apparel, Electronics, Footwear, Home Appliances   |                   |                           
| Top-N items per category                          | 10                |
| Theoretical candidate pairs                       | 2,040             |
|   = 51 sellers × 4 categories × 10 items          |                   |
-------------------------------------------------------------------------
| Seller–item pairs removed                         | 40                |
|   Reason: sellers already carried these items     |                   |
| Actual recommended rows  = 2,040 − 40             | 2,000             |
-------------------------------------------------------------------------
| Rows per category                                 | 500 each          |
|   Apparel                                         | 500               |
|   Electronics                                     | 500               |
|   Footwear                                        | 500               |
|   Home Appliances                                 | 500               |
-------------------------------------------------------------------------
| Unique items per category                         | 10 each           |
| Avg recommendations per seller (2,000 ÷ 51)       | ~39.22            |
-------------------------------------------------------------------------

Comparison Summary: Competitor vs Company Recommendations
-------------------------------------------------------------------------------------------------------------------
| Attribute                                   | Competitor Recommendation          | Company Recommendation       |
------------------------------------------------------------------------------------------------------------------
| Total Sellers                               | 51                                 | 51                           |
| Total Product Categories                    | 4                                  | 4                            |
| Top Items per Category                      | 10                                 | 10                           |
| Total Top Items Considered                  | 40 (4 categories × 10 each)        | 40 (4 categories × 10 each)  |
|                                              |                                   |                              |
| Initial Candidate Pairs (Seller × Item)     | 2,040                              | 2,040                        |
| Pairs Removed (seller already had item)     | 40                                 | 40                           |
| Final Recommendation Rows                   | 2,000                              | 2,000                        |
|                                              |                                   |                              |
| Rows Per Category                           | 500 each                           | 500 each                     |
| Distinct Items per Category                 | 10                                 | 10                           |
| Average Recommendations per Seller          | ~39.22                             | ~39.22                       |
-------------------------------------------------------------------------------------------------------------------

Excluded Seller–Item Pairs, 40 removed as sellers already sell those items.
|--------------------------|--------------------------|
| COMPANY_EXCLUDED (40)    | COMPETITOR_EXCLUDED(40)  |
| seller    item           | seller    item           |
|--------------------------|--------------------------|
| S100     i924456         | S102     i229843         |
| S101     i346048         | S102     i244311         |
| S101     i689126         | S103     i745431         |
| S103     i188721         | S105     i747482         |
| S103     i564572         | S106     i972339         |
| S104     i361264         | S107     i262130         |
| S106     i297288         | S108     i997888         |
| S108     i195367         | S109     i353652         |
| S108     i334854         | S109     i392621         |
| S109     i430676         | S114     i807347         |
| S109     i621011         | S116     i797499         |
| S112     i320216         | S117     i482869         |
| S112     i515349         | S119     i52174          |
| S114     i42871          | S120     i13688          |
|--------------------------|--------------------------|
"""
import sys
import os
from datetime import datetime
import yaml
import logging
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DoubleType, StringType

# -----------------------
# Helpers
# -----------------------
def get_latest_run_date_path(base_path: str) -> str:
    """
    Works for both local paths and S3/HDFS paths.
    Uses Spark to list run_date=YYYY-MM-DD folders safely.
    """
    # try Spark filesystem listing
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.org.apache.hadoop.fs.Path(base_path).toUri(),
            spark._jsc.hadoopConfiguration()
        )
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(base_path)

        if not fs.exists(path_obj):
            raise FileNotFoundError(f"Base path not found: {base_path}")

        statuses = fs.listStatus(path_obj)
        run_dirs = [
            s.getPath().toString()
            for s in statuses
            if s.isDirectory() and s.getPath().getName().startswith("run_date=")
        ]

        if not run_dirs:
            raise FileNotFoundError(f"No run_date folders found under: {base_path}")

        # Sort by date (descending)
        run_dirs_sorted = sorted(run_dirs, reverse=True)
        return run_dirs_sorted[0]
    except Exception as e:
        raise RuntimeError(f"Failed to list run_date folders under {base_path}: {e}")

def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_spark(app_name="consumption_recommendation", shuffle_partitions: int = 8):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    # Reduce Spark/Hudi chatter in stdout; set to ERROR to hide verbose Hudi warnings
    spark.sparkContext.setLogLevel("ERROR")
    # help with legacy two-digit year parsing / datetime rebase issues
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    # Suggested tuning for this workload
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # set a safe broadcast join threshold (adjust on cluster)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(512 * 1024 * 1024))  # 512MB
    return spark

def rename_spark_output(dir_path: str, final_name: str, spark=None):
    # Build URI-aware FileSystem
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path_obj = jvm.org.apache.hadoop.fs.Path(dir_path)

    # FS must match S3A if dir_path = s3a://
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        path_obj.toUri(),
        hadoop_conf
    )

    # Iterate files under directory
    for file in fs.listStatus(path_obj):
        name = file.getPath().getName()

        if name.startswith("part-") and name.endswith(".csv"):
            src = file.getPath()
            dst = jvm.org.apache.hadoop.fs.Path(f"{dir_path}/{final_name}")
            fs.rename(src, dst)
            print(f"✔ Renamed {name} → {final_name}")
            return

    print(f"⚠ No part-*.csv found in {dir_path} to rename")

# Configure module logger: default to ERROR to avoid printing diagnostic lines in normal runs.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
if not logger.handlers:
    # attach a NullHandler to avoid 'No handler' warnings when used as a module
    logger.addHandler(logging.NullHandler())

def write_csv_one(df, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)

def write_per_seller_files(df, out_dir):
    """
    Write recommendations per seller as individual CSV files under out_dir/<seller_id>/recommendation.csv
    This is intended for tracking recommendations per seller separately. For small number of sellers
    this loops over distinct seller_ids and writes a single-file CSV per seller. For large deployments
    consider writing partitioned output or a different storage format.
    """
    os.makedirs(out_dir, exist_ok=True)
    # collect distinct seller ids (should be small; if large, change approach)
    seller_ids = [r[0] for r in df.select("seller_id").distinct().collect()]
    # Try a driver-side write per seller using pandas to guarantee one file per seller.
    try:
        import pandas as _pd
        FINAL_COLS = ["seller_id","item_id","item_name","category","market_price","expected_units_sold","expected_revenue"]
        for sid in seller_ids:
            seller_dir = os.path.join(out_dir, str(sid))
            os.makedirs(seller_dir, exist_ok=True)
            # select only final output columns to reduce driver memory usage
            seller_df = df.filter(F.col("seller_id") == sid).select(*FINAL_COLS)
            if seller_df.rdd.isEmpty():
                # write an empty CSV with header
                cols = seller_df.schema.names
                _pd.DataFrame(columns=cols).to_csv(os.path.join(seller_dir, "recommendations.csv"), index=False)
                continue
            pdf = seller_df.toPandas()
            # write without changing filename semantics (small outputs)
            pdf.to_csv(os.path.join(seller_dir, "recommendations.csv"), index=False)
    except Exception:
        # Fallback: Spark write per seller (coalesced). This may create part files rather than a single named file.
        for sid in seller_ids:
            seller_dir = os.path.join(out_dir, str(sid))
            os.makedirs(seller_dir, exist_ok=True)
            seller_df = df.filter(F.col("seller_id") == sid)
            seller_df.coalesce(1).write.mode("overwrite").option("header", True).csv(seller_dir)

def normalize_item_id_expr(col):
    # return an expression that trims and lowercases the item_id (string)
    return F.lower(F.trim(F.col(col)).cast(StringType()))


def print_seller_item_pairs(df, title: str = None, max_rows: int = 200):
    """
    Collect a small seller,item dataframe and print a compact table to stdout.
    This is intended for human-readable diagnostics for small result sets (like 40 rows).
    """
    try:
        rows = df.select("seller_id", "item_id").dropDuplicates().orderBy("seller_id", "item_id").limit(max_rows).collect()
        if title:
            print(f"\n{title}")
        if not rows:
            print("(no rows)")
            return
        # compute column widths
        header = ["seller_id", "item_id"]
        col1w = max(len(header[0]), max(len(r[0]) for r in rows))
        col2w = max(len(header[1]), max(len(r[1]) for r in rows))
        sep = "+-" + "-" * col1w + "-+-" + "-" * col2w + "-+"
        # header
        print(sep)
        print(f"| {header[0].ljust(col1w)} | {header[1].ljust(col2w)} |")
        print(sep)
        for r in rows:
            print(f"| {str(r[0]).ljust(col1w)} | {str(r[1]).ljust(col2w)} |")
        print(sep)
    except Exception as e:
        # fallback to simple show
        try:
            print(f"{title} (fallback show):")
            df.select("seller_id", "item_id").dropDuplicates().orderBy("seller_id", "item_id").show(max_rows, truncate=False)
        except Exception:
            logger.debug("Failed to print seller-item pairs: %s", e)

# -----------------------
# Shared utility to prepare seller catalog metadata
# -----------------------
def build_item_meta(seller_catalog_df):
    """
    Returns item_meta DataFrame:
      item_id, item_name, category, catalog_market_price, num_sellers_in_catalog
    Uses first non-null item_name/category/price encountered per item_id and counts sellers.
    """
    item_meta = (
        seller_catalog_df
        .withColumn("item_id_norm", normalize_item_id_expr("item_id"))
        .withColumn("item_name", F.trim(F.col("item_name")))
        .withColumn("category", F.trim(F.col("category")))
        .groupBy("item_id_norm")
        .agg(
            F.first(F.col("item_name"), ignorenulls=True).alias("item_name"),
            F.first(F.col("category"), ignorenulls=True).alias("category"),
            F.first(F.col("marketplace_price"), ignorenulls=True).alias("catalog_market_price"),
            F.countDistinct(F.col("seller_id")).alias("num_sellers_in_catalog")
        )
        .withColumnRenamed("item_id_norm", "item_id")
    )
    return item_meta

# -----------------------
# Company recommendation routine
# -----------------------
def company_recommendation(spark, paths, cfg, top_n=10):
    print("\n==============================")
    print("   COMPANY RECOMMENDATION")
    print("==============================")

  # base path from config
    base_company_path = paths.get("gold_company_sales_data")
    # dynamically pick latest run_date folder
    comp_path = get_latest_run_date_path(base_company_path)
    print(f"[company] Using Hudi table path: {comp_path}")

    base_catalog_path = paths.get("gold_seller_catalog_data")
    seller_catalog_path = get_latest_run_date_path(base_catalog_path)
    print(f"[company] Using Seller Catalog path: {seller_catalog_path}")

    out_path = paths.get("gold_recommendation_company_data")

    # Writing company recommendation.
    run_ts = datetime.today().strftime("%Y-%m-%d_%H%M%S")
    company_run_path = os.path.join(out_path, f"run_date={run_ts}")

    if not comp_path or not seller_catalog_path or not out_path:
        raise SystemExit("❌ Missing required config paths for company recommendation (gold_company_sales_data / gold_seller_catalog_data / gold_recommendation_company_data)")

    # read Hudi snapshots (select only required columns for faster scan)
    NEEDED_COMPANY_COLS = ["item_id", "units_sold", "marketplace_price", "sale_date"]
    NEEDED_SELLER_CATALOG_COLS = ["seller_id", "item_id", "item_name", "category", "marketplace_price", "stock_qty"]

    company_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(comp_path)
        .select(*[c for c in NEEDED_COMPANY_COLS if c in spark.read.format("hudi").load(comp_path).schema.names])  # defensive select
    )
    seller_catalog_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(seller_catalog_path)
        .select(*[c for c in NEEDED_SELLER_CATALOG_COLS if c in spark.read.format("hudi").load(seller_catalog_path).schema.names])  # defensive select
    )

    print(f"Loaded company rows    : {company_df.count()}")
    print(f"Loaded seller rows     : {seller_catalog_df.count()}")

    # Normalize item_id in both frames to ensure joins succeed
    company = (
        company_df
        .withColumn("item_id", normalize_item_id_expr("item_id"))
        .withColumn("units_sold", F.coalesce(F.col("units_sold").cast(DoubleType()), F.lit(0.0)))
        .withColumn("marketplace_price", F.col("marketplace_price").cast(DoubleType()))
    )

    seller_catalog = (
        seller_catalog_df
        .withColumn("item_id", normalize_item_id_expr("item_id"))
        .withColumn("seller_id", F.trim(F.col("seller_id")).cast(StringType()))
        .withColumn("marketplace_price", F.col("marketplace_price").cast(DoubleType()))
    )

    # cache seller_catalog as it is re-used multiple times
    seller_catalog = seller_catalog.cache()

    # Build item metadata from seller catalog
    item_meta = build_item_meta(seller_catalog)
    item_meta = item_meta.cache()

    # Aggregate company: total units per item and company average price
    comp_agg = (
        company.groupBy("item_id")
        .agg(
            F.sum("units_sold").alias("company_total_units"),
            F.avg("marketplace_price").alias("company_avg_price")
        )
    )

    # Enrich agg with metadata (inner join to exclude items not in seller catalog)
    # Use broadcast for small item_meta
    comp_enriched = (
        comp_agg.join(F.broadcast(item_meta), on="item_id", how="inner")
        .withColumn("market_price", F.coalesce(F.col("catalog_market_price"), F.col("company_avg_price")))
        .filter(F.col("category").isNotNull())
        .filter(F.col("item_name").isNotNull())
        .filter(F.col("market_price").isNotNull())
        .select("item_id", "item_name", "category", "company_total_units", "market_price", "num_sellers_in_catalog")
    )

    # Rank top N per category
    win = Window.orderBy(F.desc("company_total_units"))

    comp_top = (
        comp_enriched
            .withColumn("rn", F.row_number().over(win))
            .filter(F.col("rn") <= top_n)
            .drop("rn")
            .dropDuplicates(["item_id"])
    )

    print("\nCompany top-{} items per category:".format(top_n))
    comp_top.orderBy(F.desc("company_total_units")).show(30, truncate=False)

    # Prepare sellers list (distinct)
    sellers_df = seller_catalog.select("seller_id").distinct().cache()

    # We need top items as a DataFrame; ensure item_id column name matches seller_catalog for joins
    top_items_df = comp_top.select("item_id", "item_name", "category", "company_total_units", "market_price").cache()

    # --- diagnostic prints for traceability ---
    print("\n[company] Global Top-{} items (across all categories):".format(top_n))
    try:
        comp_top.orderBy(F.desc("company_total_units")).show(20, truncate=False)
    except Exception:
        print("[company] Unable to show top-N group counts (Spark show failed)")
    
    sellers_count_total = sellers_df.count()
    topn_count = top_items_df.count()
    candidate_pairs_total = sellers_count_total * topn_count
    print(f"[company] sellers_count={sellers_count_total}, topn_count={topn_count}, candidate_pairs_total={candidate_pairs_total}")
    
    # Create cartesian sellers x top_items and find missing using left_anti (clean & unambiguous)
    sellers_top = sellers_df.crossJoin(top_items_df).persist()

    # Build a small table of seller->item pairs present (normalized item_id)
    seller_present = seller_catalog.select("seller_id", "item_id").distinct().cache()

    # --- Save excluded (already-owned) pairs for diagnostics: inner join of sellers_top and seller_present ---
    try:
        excluded = sellers_df.crossJoin(top_items_df).join(
            seller_present,
            on=["seller_id", "item_id"],
            how="inner"
        ).select("seller_id", "item_id", "item_name", "category", "company_total_units", "market_price")
        excluded_count = excluded.count()
        logger.debug("[company] already-owned pairs (excluded) count: %s", excluded_count)

        # excluded pairs also inside run_date folder
        # --- Save excluded pairs similar to per_seller logic ---
        excluded_out_dir = os.path.join(company_run_path, "excluded_pairs")

        # 1. Write full excluded rows (Spark will create the folder)
        write_csv_one(excluded, excluded_out_dir)

        # 2. Simplified rows (no os.makedirs() anywhere)
        excluded_simple = excluded.select("seller_id", "item_id")\
                          .dropDuplicates()\
                          .orderBy("seller_id", "item_id")

        excluded_simple_dir = os.path.join(excluded_out_dir, "simple")
        write_csv_one(excluded_simple, excluded_simple_dir)

        print(f"[COMPANY] Excluded pairs written to: {excluded_out_dir}")

        try:
           # human-readable print
            print_seller_item_pairs(excluded_simple, title="Company - Excluded seller,item pairs")
        except Exception:
            logger.debug("[company] Could not write simplified excluded pairs")
    except Exception as e:
        print(f"  [company] Failed to write excluded pairs: {e}")
        excluded_count = 0

    # Use left_anti to find missing: those in sellers_top but not in seller_present
    missing = sellers_top.join(
        seller_present,
        on=[sellers_top.seller_id == seller_present.seller_id, sellers_top.item_id == seller_present.item_id],
        how="left_anti" # inner exclusion of already-owned items
    ).select(
        sellers_top.seller_id.alias("seller_id"),
        sellers_top.item_id.alias("item_id"),
        sellers_top.item_name.alias("item_name"),
        sellers_top.category.alias("category"),
        sellers_top.company_total_units.alias("company_total_units"),
        sellers_top.market_price.alias("market_price")
    )

    # (Note: competitor excluded-pairs are handled in competitor_recommendation)

    # Compute sellers_selling_count (how many distinct sellers currently sell the item)
    sellers_selling_per_item = seller_present.groupBy("item_id").agg(F.countDistinct("seller_id").alias("sellers_selling_count"))

    # Join counts into missing
    missing = missing.join(sellers_selling_per_item, on="item_id", how="left")
    # If no sellers currently selling an item (sellers_selling_count null), set to 1 to avoid divide-by-zero
    missing = missing.withColumn("sellers_selling_count", F.when(F.col("sellers_selling_count").isNull(), F.lit(1)).otherwise(F.col("sellers_selling_count")))

    # expected_units_sold = company_total_units / sellers_selling_count
    # Formula: expected_units_sold = total_units_sold_for_item / number_of_sellers_selling_this_item
    # - total_units_sold_for_item  => company_total_units (aggregated from company sales)
    # - number_of_sellers_selling_this_item => sellers_selling_count (count distinct sellers from seller catalog)
    # Implementation: divide and cast to double; we use a fallback sellers_selling_count=1 earlier to avoid divide-by-zero.
    missing = missing.withColumn("expected_units_sold", (F.col("company_total_units") / F.col("sellers_selling_count")).cast(DoubleType()))

    # ensure market_price numeric and fallback to 0.0 if missing
    missing = missing.withColumn("market_price", F.col("market_price").cast(DoubleType()))
    missing = missing.withColumn("market_price", F.when(F.col("market_price").isNull(), F.lit(0.0)).otherwise(F.col("market_price")))

    # expected_revenue
    # Formula: expected_revenue = expected_units_sold * market_price
    # - expected_units_sold computed above
    # - market_price comes from seller catalog (catalog_market_price) or company average price (company_avg_price)
    missing = missing.withColumn("expected_revenue", (F.col("expected_units_sold") * F.col("market_price")).cast(DoubleType()))

    # Final selection & write
    reco_cols = ["seller_id", "item_id", "item_name", "category", "market_price", "expected_units_sold", "expected_revenue"]
    recos = missing.select(*reco_cols).persist()

    print(f"\n💾 Writing company recommendation → {company_run_path}")
    write_csv_one(recos, company_run_path)

    output_cfg = cfg.get("recommendation_files", {})
    company_file_name = output_cfg.get("company_recommendation_file")

    rename_spark_output(company_run_path, company_file_name, spark)

    # Additionally write per-seller recommendation files for traceability.
    # Directory: <out_path>/per_seller/<seller_id>/recommendation.csv
    # try:
        # per-seller folder inside the same run_date directory
        #per_seller_dir = os.path.join(company_run_path, "per_seller")
        #write_per_seller_files(recos, per_seller_dir)

        #print(f"  [COMPANY] Per-seller recommendations written to: {per_seller_dir}")
    # except Exception as e:
        # print(f"  [COMPANY] Failed to write per-seller recommendations: {e}")

    # include excluded count in summary if available
    try:
        excluded_count = int(excluded_count)
    except Exception:
        excluded_count = 0

    # Summary metrics
    total_sellers = sellers_df.count()
    total_top_items = top_items_df.count()
    total_recos = recos.count()
    null_price_count = recos.filter(F.col("market_price") == 0.0).count()
    null_itemname_count = recos.filter(F.col("item_name").isNull()).count()
    total_categories = top_items_df.select("category").distinct().count()
    items_per_category_df = top_items_df.groupBy("category").count()
    items_per_category = items_per_category_df.select(F.max("count")).collect()[0][0]
    avg_per_seller = total_recos / float(max(1, total_sellers))     
    theoretical_total = total_sellers * top_n
    duplicate_count = excluded_count if 'excluded_count' in locals() else 0

    print("\n===== COMPANY RECOMMENDATION SUMMARY =====")
    print(f"Total sellers                 : {total_sellers}")
    print(f"Total categories              : {total_categories}")
    print(f"Top-N items per category      : {items_per_category}")
    print(f"Theoretical total combinations     : {theoretical_total} "
      f"(= {total_sellers} × {total_categories} × {items_per_category})")
    print(f"Seller–item pairs removed (dupes) : {duplicate_count}")
    print(f"Total recommendations written : {total_recos}")

    print(f"Total company top items    : {total_top_items}")
    print(f"Avg recommendations per seller: {avg_per_seller:.2f}")

    print("\nSample recommendations (up to 20):")
    recos.show(20, truncate=False)

    # cleanup persisted frames
    try:
        recos.unpersist()
        sellers_top.unpersist()
        seller_present.unpersist()
        item_meta.unpersist()
        seller_catalog.unpersist()
        sellers_df.unpersist()
        top_items_df.unpersist()
    except Exception:
        pass

    return {
        "total_sellers": total_sellers,
        "top_items": total_top_items,
        "recommendations": total_recos,
        "null_price_count": null_price_count,
        "null_itemname_count": null_itemname_count
    }

# -----------------------
# Competitor recommendation routine (same logic but using competitor top items)
# -----------------------
def competitor_recommendation(spark, paths, cfg, top_n=10):
    print("\n==============================")
    print(" COMPETITOR RECOMMENDATION")
    print("==============================")

    base_comp_path = paths.get("gold_competitor_sales_data")
    comp_path = get_latest_run_date_path(base_comp_path)
    print(f"[competitor] Using Hudi path: {comp_path}")

    base_catalog_path = paths.get("gold_seller_catalog_data")
    seller_catalog_path = get_latest_run_date_path(base_catalog_path)
    print(f"[competitor] Using Seller Catalog path: {seller_catalog_path}")

    out_path = paths.get("gold_recommendation_competitor_data")

    # Writing competitor recommendation.
    run_ts = datetime.today().strftime("%Y-%m-%d_%H%M%S")
    competitor_run_path = os.path.join(out_path, f"run_date={run_ts}")

    if not comp_path or not seller_catalog_path or not out_path:
        raise SystemExit("❌ Missing required config paths for competitor recommendation (gold_competitor_sales_data / gold_seller_catalog_data / gold_recommendation_competitor_data)")

    # read Hudi snapshots (select only required columns for faster scan)
    NEEDED_COMPETITOR_COLS = ["seller_id", "item_id", "units_sold", "marketplace_price", "sale_date"]
    NEEDED_SELLER_CATALOG_COLS = ["seller_id", "item_id", "item_name", "category", "marketplace_price", "stock_qty"]

    competitor_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(comp_path)
        .select(*[c for c in NEEDED_COMPETITOR_COLS if c in spark.read.format("hudi").load(comp_path).schema.names])  # defensive select
    )
    seller_catalog_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(seller_catalog_path)
        .select(*[c for c in NEEDED_SELLER_CATALOG_COLS if c in spark.read.format("hudi").load(seller_catalog_path).schema.names])  # defensive select
    )

    print(f"Loaded competitor rows : {competitor_df.count()}")
    print(f"Loaded seller rows     : {seller_catalog_df.count()}")

    competitor = (
        competitor_df
        .withColumn("item_id", normalize_item_id_expr("item_id"))
        .withColumn("units_sold", F.coalesce(F.col("units_sold").cast(DoubleType()), F.lit(0.0)))
        .withColumn("marketplace_price", F.col("marketplace_price").cast(DoubleType()))
    )

    seller_catalog = (
        seller_catalog_df
        .withColumn("item_id", normalize_item_id_expr("item_id"))
        .withColumn("seller_id", F.trim(F.col("seller_id")).cast(StringType()))
        .withColumn("marketplace_price", F.col("marketplace_price").cast(DoubleType()))
    )

    # cache seller_catalog and item_meta for reuse
    seller_catalog = seller_catalog.cache()
    item_meta = build_item_meta(seller_catalog).cache()

    comp_agg = (
        competitor.groupBy("item_id")
        .agg(
            F.sum("units_sold").alias("comp_total_units"),
            F.avg("marketplace_price").alias("competitor_avg_price")
        )
    )

    # Derive market_price for each item using catalog price when available, otherwise competitor average price
    # Use broadcast for item_meta
    comp_enriched = (
        comp_agg.join(F.broadcast(item_meta), on="item_id", how="inner")
        .withColumn("market_price", F.coalesce(F.col("catalog_market_price"), F.col("competitor_avg_price")))
        .filter(F.col("category").isNotNull())
        .filter(F.col("item_name").isNotNull())
        .filter(F.col("market_price").isNotNull())
        .select("item_id", "item_name", "category", "comp_total_units", "market_price", "num_sellers_in_catalog")
    )

    win = Window.orderBy(F.desc("comp_total_units"))

    comp_top = (
        comp_enriched
            .withColumn("rn", F.row_number().over(win))
            .filter(F.col("rn") <= top_n)
            .drop("rn")
            .dropDuplicates(["item_id"])
    )
    print("\nCompetitor top-{} items per category:".format(top_n))
    comp_top.orderBy(F.desc("comp_total_units")).show(30, truncate=False)

    sellers_df = seller_catalog.select("seller_id").distinct().cache()
    top_items_df = comp_top.select("item_id", "item_name", "category", F.col("comp_total_units").alias("total_units"), "market_price").cache()
    
    # --- diagnostic prints for traceability (competitor) ---
    print("\n[competitor] Global Top-{} competitor items:".format(top_n))
    try:
        comp_top.orderBy(F.desc("comp_total_units")).show(20, truncate=False)
    except Exception:
        print("[competitor] Unable to show top-N group counts (Spark show failed)")

    sellers_count_total = sellers_df.count()
    topn_count = top_items_df.count()
    candidate_pairs_total = sellers_count_total * topn_count
    print(f"[competitor] sellers_count={sellers_count_total}, topn_count={topn_count}, candidate_pairs_total={candidate_pairs_total}")
    
    sellers_top = sellers_df.crossJoin(top_items_df).persist()
    seller_present = seller_catalog.select("seller_id", "item_id").distinct().cache()

    # initialize excluded count so it's always defined for summary
    excluded_comp_count = 0

    # --- Save excluded (already-owned) pairs for diagnostics (competitor) ---
    try:
        excluded_comp = sellers_df.crossJoin(top_items_df).join(
            seller_present,
            on=["seller_id", "item_id"],
            how="inner"
        ).select("seller_id", "item_id", "item_name", "category", "total_units", "market_price")
        excluded_comp_count = excluded_comp.count()
        logger.debug("[competitor] already-owned pairs (excluded) count: %s", excluded_comp_count)

        # excluded pairs inside run_date folder
        excluded_comp_out_dir = os.path.join(competitor_run_path, "excluded_pairs") 
        write_csv_one(excluded_comp, excluded_comp_out_dir)

        try:
            excluded_comp_simple = excluded_comp.select("seller_id", "item_id").dropDuplicates().orderBy("seller_id", "item_id")
            write_csv_one(excluded_comp_simple, os.path.join(excluded_comp_out_dir, "simple"))
            logger.debug("[competitor] wrote simplified excluded seller,item pairs to %s", os.path.join(excluded_comp_out_dir, "simple"))
            # Print a compact table of seller,item pairs for quick human inspection
            print_seller_item_pairs(excluded_comp_simple, title="Competitor - Excluded seller,item pairs")
        except Exception:
            logger.debug("[competitor] Could not write simplified excluded pairs")
    except Exception as e:
        print(f"  [competitor] Failed to write excluded pairs: {e}")
        excluded_comp_count = 0

    missing = sellers_top.join(
        seller_present,
        on=[sellers_top.seller_id == seller_present.seller_id, sellers_top.item_id == seller_present.item_id],
        how="left_anti" # inner exclusion of already-owned items.
    ).select(
        sellers_top.seller_id.alias("seller_id"),
        sellers_top.item_id.alias("item_id"),
        sellers_top.item_name.alias("item_name"),
        sellers_top.category.alias("category"),
        sellers_top.total_units.alias("total_units"),
        sellers_top.market_price.alias("market_price")
    )
    
    missing_count = missing.count()
    already_have_pairs = candidate_pairs_total - missing_count
    print(f"[competitor] missing_count={missing_count}, already_have_pairs_removed={already_have_pairs}")
    try:
        distinct_categories = top_items_df.select("category").distinct().count()
        avg_rows_per_seller = float(missing_count) / float(max(1, sellers_count_total))
        print(f"[competitor] distinct_sellers={sellers_count_total}, distinct_categories={distinct_categories}")
        print(f"[competitor] final_recommendations={missing_count}, avg_rows_per_seller={avg_rows_per_seller:.2f}")
    except Exception:
        pass

    sellers_selling_per_item = seller_present.groupBy("item_id").agg(F.countDistinct("seller_id").alias("sellers_selling_count"))
    missing = missing.join(sellers_selling_per_item, on="item_id", how="left")
    missing = missing.withColumn("sellers_selling_count", F.when(F.col("sellers_selling_count").isNull(), F.lit(1)).otherwise(F.col("sellers_selling_count")))
    # Formula: expected_units_sold = total_units_sold_for_item / number_of_sellers_selling_this_item
    # - total_units_sold_for_item => total_units (aggregated from competitor sales)
    # - number_of_sellers_selling_this_item => sellers_selling_count (count distinct sellers from seller catalog)
    # Note: we previously replaced null sellers_selling_count with 1 to avoid divide-by-zero.
    missing = missing.withColumn("expected_units_sold", (F.col("total_units") / F.col("sellers_selling_count")).cast(DoubleType()))
    missing = missing.withColumn("market_price", F.col("market_price").cast(DoubleType()))
    missing = missing.withColumn("market_price", F.when(F.col("market_price").isNull(), F.lit(0.0)).otherwise(F.col("market_price")))
    missing = missing.withColumn("expected_revenue", (F.col("expected_units_sold") * F.col("market_price")).cast(DoubleType()))

    reco_cols = ["seller_id", "item_id", "item_name", "category", "market_price", "expected_units_sold", "expected_revenue"]
    recos = missing.select(*reco_cols).persist()

    print(f"\n💾 Writing competitor recommendation → {competitor_run_path}")
    write_csv_one(recos, competitor_run_path)

    output_cfg = cfg.get("recommendation_files", {})
    competitor_file_name = output_cfg.get("competitor_recommendation_file")

    rename_spark_output(competitor_run_path, competitor_file_name, spark)

    # Additionally write per-seller recommendation files for traceability
    #try:
        # per-seller folder inside same run_date directory
        #per_seller_dir = os.path.join(competitor_run_path, "per_seller")
        #write_per_seller_files(recos, per_seller_dir)
        #print(f"  [COMPETITOR] Per-seller recommendations written to: {per_seller_dir}")
    #except Exception as e:
        #print(f"  [COMPETITOR] Failed to write per-seller recommendations: {e}")

    total_sellers = sellers_df.count()
    total_top_items = top_items_df.count()
    total_recos = recos.count()
    null_price_count = recos.filter(F.col("market_price") == 0.0).count()
    null_itemname_count = recos.filter(F.col("item_name").isNull()).count()
    total_categories = top_items_df.select("category").distinct().count()
    items_per_category_df = top_items_df.groupBy("category").count()
    items_per_category = items_per_category_df.select(F.max("count")).collect()[0][0]
    avg_per_seller = total_recos / float(max(1, total_sellers))     
    theoretical_total = total_sellers * top_n
    duplicate_count = excluded_comp_count if 'excluded_comp_count' in locals() else 0

    print("\n===== COMPETITOR RECOMMENDATION SUMMARY =====")
    print(f"Total sellers                 : {total_sellers}")
    print(f"Total categories              : {total_categories}")
    print(f"Top-N items per category      : {items_per_category}")
    print(f"Theoretical total combinations     : {theoretical_total} "
      f"(= {total_sellers} × {total_categories} × {items_per_category})")
    print(f"Seller–item pairs removed (dupes) : {duplicate_count}")
    print(f"Total recommendations written : {total_recos}")

    print(f"Total company top items    : {total_top_items}")
    print(f"Avg recommendations per seller: {avg_per_seller:.2f}")

    print("\nSample recommendations (up to 20):")
    recos.show(20, truncate=False)

    # cleanup persisted frames
    try:
        recos.unpersist()
        sellers_top.unpersist()
        seller_present.unpersist()
        item_meta.unpersist()
        seller_catalog.unpersist()
        sellers_df.unpersist()
        top_items_df.unpersist()
    except Exception:
        pass

    return {
        "total_sellers": total_sellers,
        "top_items": total_top_items,
        "recommendations": total_recos,
        "null_price_count": null_price_count,
        "null_itemname_count": null_itemname_count
    }

# -----------------------
# Sub-functions for easy invocation
# -----------------------
def run_company_only(spark, paths, cfg):
    print("\n===== RUNNING COMPANY ONLY =====")
    return company_recommendation(
        spark,
        paths,
        cfg,    
        top_n=cfg.get("recommendation", {}).get("top_n", 10)
    )

def run_competitor_only(spark, paths, cfg):
    print("\n===== RUNNING COMPETITOR ONLY =====")
    return competitor_recommendation(
        spark,
        paths,
        cfg,
        top_n=cfg.get("recommendation", {}).get("top_n", 10)
    )

# -----------------------
# MAIN
# -----------------------
def main():
    if "--config" not in sys.argv:
        print("Usage: consumption_recommendation.py --config <config.yml> [--mode company|competitor|both]")
        sys.exit(1)

    cfg_path = sys.argv[sys.argv.index("--config") + 1]
    mode = "both"   # default
    if "--mode" in sys.argv:
        mode = sys.argv[sys.argv.index("--mode") + 1].lower()

    if not os.path.exists(cfg_path):
        print(f"❌ Config file not found: {cfg_path}")
        sys.exit(1)

    cfg = load_yaml(cfg_path)
    paths = cfg.get("paths", {})

      # --- JOB TIMER START ---
    job_start_ts = time.time()
    job_start_dt = datetime.now()
    logger.error(f"\n[TIMER] JOB started at: {job_start_dt}")
    print(f"[TIMER] JOB started at: {job_start_dt}")
    # ------------------------

    spark = get_spark(
        "ConsumptionRecommendation",
        shuffle_partitions=cfg.get("spark", {}).get("shuffle_partitions", 8)
    )

    if mode == "company":
        result_company = run_company_only(spark, paths, cfg)
        print("✔ Completed company-only recommendation.")
        spark.stop()
        return

    elif mode == "competitor":
        result_competitor = run_competitor_only(spark, paths, cfg)
        print("✔ Completed competitor-only recommendation.")
        spark.stop()
        return

    else:  # both
        result_company = run_company_only(spark, paths, cfg)
        result_competitor = run_competitor_only(spark, paths, cfg)

        print("\n==============================")
        print(" ALL RECOMMENDATIONS COMPLETE")
        print("==============================")
        print("Company metrics   :", result_company)
        print("Competitor metrics:", result_competitor)

        # --- JOB TIMER END ---
        job_end_ts = time.time()
        job_end_dt = datetime.now()
        job_duration = job_end_ts - job_start_ts
        logger.error(f"\n[TIMER] JOB finished at: {job_end_dt}, Duration: {job_duration:.2f} seconds")
        print(f"[TIMER] JOB finished at: {job_end_dt}, Duration: {job_duration:.2f} seconds")
        # ----------------------

        spark.stop()

if __name__ == "__main__":
    main()