#!/usr/bin/env python3
"""
consumption_recommendation.py

This script generates item recommendations for sellers based on both company and competitor sales data. The recommendations are aimed at identifying missing top-selling items and estimating their potential revenue for sellers to add to their catalog.

============================
Company-Specific Objectives:
============================
1. **Identify top-selling items within the company:**
   - Analyze all sellers’ sales data to determine top-selling items by category.

2. **Compare seller catalogs against top-selling items:**
   - Identify missing items in each seller’s catalog from the company’s top sellers.

3. **Compute business metrics for recommendations:**
   - Calculate current price from company data.
   - Estimate expected revenue if the seller adds the item:
     - `expected_revenue = expected_units_sold * marketplace_price`

==============================
Competitor-Specific Objectives:
==============================
1. **Analyze competitor data:**
   - Identify top-selling items in the market based on competitor sales.

2. **Recommend items missing from the company’s catalog:**
   - Identify high-performing items in the competitor's catalog that are missing from the company catalog.

3. **Compute business metrics for recommendations:**
   - Calculate current price from competitor data.
   - Estimate expected revenue if the seller adds the item:
     - `expected_revenue = expected_units_sold * marketplace_price`

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
"""
import sys
import os
from datetime import datetime
import yaml

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DoubleType, StringType

# -----------------------
# Helpers
# -----------------------
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
    spark.sparkContext.setLogLevel("WARN")
    # help with legacy two-digit year parsing / datetime rebase issues
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    return spark

def write_csv_one(df, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)

def normalize_item_id_expr(col):
    # return an expression that trims and lowercases the item_id (string)
    return F.lower(F.trim(F.col(col)).cast(StringType()))

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
def company_recommendation(spark, paths, top_n=10):
    print("\n==============================")
    print("   COMPANY RECOMMENDATION")
    print("==============================")

    comp_path = paths.get("gold_company_sales_data")
    seller_catalog_path = paths.get("gold_seller_catalog_data")
    out_path = paths.get("gold_recommendation_company_data")

    if not comp_path or not seller_catalog_path or not out_path:
        raise SystemExit("❌ Missing required config paths for company recommendation (gold_company_sales_data / gold_seller_catalog_data / gold_recommendation_company_data)")

    # read Hudi snapshots
    company_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(comp_path)
    )
    seller_catalog_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(seller_catalog_path)
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

    # Build item metadata from seller catalog
    item_meta = build_item_meta(seller_catalog)

    # Aggregate company: total units per item and company average price
    comp_agg = (
        company.groupBy("item_id")
        .agg(
            F.sum("units_sold").alias("company_total_units"),
            F.avg("marketplace_price").alias("company_avg_price")
        )
    )

    # Enrich agg with metadata (inner join to exclude items not in seller catalog)
    comp_enriched = (
        comp_agg.join(item_meta, on="item_id", how="inner")
        .withColumn("market_price", F.coalesce(F.col("catalog_market_price"), F.col("company_avg_price")))
        .filter(F.col("category").isNotNull())
        .filter(F.col("item_name").isNotNull())
        .filter(F.col("market_price").isNotNull())
        .select("item_id", "item_name", "category", "company_total_units", "market_price", "num_sellers_in_catalog")
    )

    # Rank top N per category
    win = Window.partitionBy("category").orderBy(F.desc("company_total_units"))
    comp_top = (
        comp_enriched
        .withColumn("rn", F.row_number().over(win))
        .filter(F.col("rn") <= top_n)
        .drop("rn")
        .dropDuplicates(["category", "item_id"])
    )

    print("\nCompany top-{} items per category:".format(top_n))
    comp_top.orderBy(F.desc("company_total_units")).show(30, truncate=False)

    # Prepare sellers list (distinct)
    sellers_df = seller_catalog.select("seller_id").distinct()

    # We need top items as a DataFrame; ensure item_id column name matches seller_catalog for joins
    top_items_df = comp_top.select("item_id", "item_name", "category", "company_total_units", "market_price")

    # --- diagnostic prints for traceability ---
    print("\n[company] Top-N per category (category -> count):")
    try:
        comp_top.groupBy("category").count().orderBy("category").show(truncate=False)
    except Exception:
        print("[company] Unable to show top-N group counts (Spark show failed)")
    
    sellers_count_total = sellers_df.count()
    topn_count = top_items_df.count()
    candidate_pairs_total = sellers_count_total * topn_count
    print(f"[company] sellers_count={sellers_count_total}, topn_count={topn_count}, candidate_pairs_total={candidate_pairs_total}")
    
    # Create cartesian sellers x top_items and find missing using left_anti (clean & unambiguous)
    sellers_top = sellers_df.crossJoin(top_items_df)

    # Build a small table of seller->item pairs present (normalized item_id)
    seller_present = seller_catalog.select("seller_id", "item_id").distinct()

    # Use left_anti to find missing: those in sellers_top but not in seller_present
    missing = sellers_top.join(
        seller_present,
        on=[sellers_top.seller_id == seller_present.seller_id, sellers_top.item_id == seller_present.item_id],
        how="left_anti"
    ).select(
        sellers_top.seller_id.alias("seller_id"),
        sellers_top.item_id.alias("item_id"),
        sellers_top.item_name.alias("item_name"),
        sellers_top.category.alias("category"),
        sellers_top.company_total_units.alias("company_total_units"),
        sellers_top.market_price.alias("market_price")
    )

    # Compute sellers_selling_count (how many distinct sellers currently sell the item)
    sellers_selling_per_item = seller_present.groupBy("item_id").agg(F.countDistinct("seller_id").alias("sellers_selling_count"))

    # Join counts into missing
    missing = missing.join(sellers_selling_per_item, on="item_id", how="left")
    # If no sellers currently selling an item (sellers_selling_count null), set to 1 to avoid divide-by-zero
    missing = missing.withColumn("sellers_selling_count", F.when(F.col("sellers_selling_count").isNull(), F.lit(1)).otherwise(F.col("sellers_selling_count")))

    # expected_units_sold = company_total_units / sellers_selling_count
    missing = missing.withColumn("expected_units_sold", (F.col("company_total_units") / F.col("sellers_selling_count")).cast(DoubleType()))

    # ensure market_price numeric and fallback to 0.0 if missing
    missing = missing.withColumn("market_price", F.col("market_price").cast(DoubleType()))
    missing = missing.withColumn("market_price", F.when(F.col("market_price").isNull(), F.lit(0.0)).otherwise(F.col("market_price")))

    # expected_revenue
    missing = missing.withColumn("expected_revenue", (F.col("expected_units_sold") * F.col("market_price")).cast(DoubleType()))

    # Final selection & write
    reco_cols = ["seller_id", "item_id", "item_name", "category", "market_price", "expected_units_sold", "expected_revenue"]
    recos = missing.select(*reco_cols)

    print("\n💾 Writing company recommendation → {}".format(out_path))
    write_csv_one(recos, out_path)

    # Summary metrics
    total_sellers = sellers_df.count()
    total_top_items = top_items_df.count()
    total_recos = recos.count()
    null_price_count = recos.filter(F.col("market_price") == 0.0).count()
    null_itemname_count = recos.filter(F.col("item_name").isNull()).count()

    print("\n===== COMPANY RECOMMENDATION SUMMARY =====")
    print(f"Total sellers                 : {total_sellers}")
    print(f"Total company top items rows  : {total_top_items}")
    print(f"Total recommendations written : {total_recos}")
    print(f"Recommendations with price=0  : {null_price_count}")
    print(f"Recommendations with no item_name: {null_itemname_count}")
    print("Sample recommendations (up to 20):")
    recos.show(20, truncate=False)

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
def competitor_recommendation(spark, paths, top_n=10):
    print("\n==============================")
    print(" COMPETITOR RECOMMENDATION")
    print("==============================")

    comp_path = paths.get("gold_competitor_sales_data")
    seller_catalog_path = paths.get("gold_seller_catalog_data")
    out_path = paths.get("gold_recommendation_competitor_data")

    if not comp_path or not seller_catalog_path or not out_path:
        raise SystemExit("❌ Missing required config paths for competitor recommendation (gold_competitor_sales_data / gold_seller_catalog_data / gold_recommendation_competitor_data)")

    competitor_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(comp_path)
    )
    seller_catalog_df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(seller_catalog_path)
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

    item_meta = build_item_meta(seller_catalog)

    comp_agg = (
        competitor.groupBy("item_id")
        .agg(
            F.sum("units_sold").alias("comp_total_units"),
            F.avg("marketplace_price").alias("competitor_avg_price")
        )
    )

    comp_enriched = (
        comp_agg.join(item_meta, on="item_id", how="inner")
        .withColumn("market_price", F.coalesce(F.col("catalog_market_price"), F.col("competitor_avg_price")))
        .filter(F.col("category").isNotNull())
        .filter(F.col("item_name").isNotNull())
        .filter(F.col("market_price").isNotNull())
        .select("item_id", "item_name", "category", "comp_total_units", "market_price", "num_sellers_in_catalog")
    )

    win = Window.partitionBy("category").orderBy(F.desc("comp_total_units"))
    comp_top = (
        comp_enriched
        .withColumn("rn", F.row_number().over(win))
        .filter(F.col("rn") <= top_n)
        .drop("rn")
        .dropDuplicates(["category", "item_id"])
    )

    print("\nCompetitor top-{} items per category:".format(top_n))
    comp_top.orderBy(F.desc("comp_total_units")).show(30, truncate=False)

    sellers_df = seller_catalog.select("seller_id").distinct()
    top_items_df = comp_top.select("item_id", "item_name", "category", F.col("comp_total_units").alias("total_units"), "market_price")
    
    # --- diagnostic prints for traceability (competitor) ---
    print("\n[competitor] Top-N per category (category -> count):")
    try:
        comp_top.groupBy("category").count().orderBy("category").show(truncate=False)
    except Exception:
        print("[competitor] Unable to show top-N group counts (Spark show failed)")

    sellers_count_total = sellers_df.count()
    topn_count = top_items_df.count()
    candidate_pairs_total = sellers_count_total * topn_count
    print(f"[competitor] sellers_count={sellers_count_total}, topn_count={topn_count}, candidate_pairs_total={candidate_pairs_total}")
    
    sellers_top = sellers_df.crossJoin(top_items_df)
    seller_present = seller_catalog.select("seller_id", "item_id").distinct()

    missing = sellers_top.join(
        seller_present,
        on=[sellers_top.seller_id == seller_present.seller_id, sellers_top.item_id == seller_present.item_id],
        how="left_anti"
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
    missing = missing.withColumn("expected_units_sold", (F.col("total_units") / F.col("sellers_selling_count")).cast(DoubleType()))
    missing = missing.withColumn("market_price", F.col("market_price").cast(DoubleType()))
    missing = missing.withColumn("market_price", F.when(F.col("market_price").isNull(), F.lit(0.0)).otherwise(F.col("market_price")))
    missing = missing.withColumn("expected_revenue", (F.col("expected_units_sold") * F.col("market_price")).cast(DoubleType()))

    reco_cols = ["seller_id", "item_id", "item_name", "category", "market_price", "expected_units_sold", "expected_revenue"]
    recos = missing.select(*reco_cols)

    print("\n💾 Writing competitor recommendation → {}".format(out_path))
    write_csv_one(recos, out_path)

    total_sellers = sellers_df.count()
    total_top_items = top_items_df.count()
    total_recos = recos.count()
    null_price_count = recos.filter(F.col("market_price") == 0.0).count()
    null_itemname_count = recos.filter(F.col("item_name").isNull()).count()

    print("\n===== COMPETITOR RECOMMENDATION SUMMARY =====")
    print(f"Total sellers                 : {total_sellers}")
    print(f"Total competitor top items    : {total_top_items}")
    print(f"Total recommendations written : {total_recos}")
    print(f"Recommendations with price=0  : {null_price_count}")
    print(f"Recommendations with no item_name: {null_itemname_count}")
    print("Sample recommendations (up to 20):")
    recos.show(20, truncate=False)

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
        top_n=cfg.get("recommendation", {}).get("top_n", 10)
    )

def run_competitor_only(spark, paths, cfg):
    print("\n===== RUNNING COMPETITOR ONLY =====")
    return competitor_recommendation(
        spark,
        paths,
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
        spark.stop()

if __name__ == "__main__":
    main()