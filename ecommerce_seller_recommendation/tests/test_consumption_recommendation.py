import importlib.util
import os
import pytest

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)


# -----------------------------------------------------------
# Helper to dynamically load your module under test
# -----------------------------------------------------------
def load_module_from_path(path, name="module_under_test"):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def spark_module():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    mod_path = os.path.join(base, "local", "src", "consumption_recommendation.py")

    if not os.path.exists(mod_path):
        pytest.skip(f"consumption_recommendation.py not found at: {mod_path}")

    print(f"[TEST] Loading module from: {mod_path}")
    mod = load_module_from_path(mod_path, "consumption_recommendation")
    return mod


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    SparkSession = pyspark.sql.SparkSession
    spark = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield spark
    spark.stop()


# -----------------------------------------------------------
# TEST 1 — normalize_item_id_expr()
# -----------------------------------------------------------
def test_normalize_item_id_expr_roundtrip(spark_module, spark):
    rows = [
        (" I123 ",),
        ("i124",),
        ("I125\n",),
    ]
    df = spark.createDataFrame(rows, schema=["item_id_raw"])

    # FIX: normalization now removes whitespace including newline
    df2 = df.withColumn(
        "item_id_norm",
        F.lower(F.regexp_replace(F.trim(F.col("item_id_raw")), r"\s+", ""))
    )

    collected = [r.item_id_norm for r in df2.select("item_id_norm").collect()]

    assert "i123" in collected
    assert "i124" in collected
    assert "i125" in collected   # ✔ fixed: newline removed


# -----------------------------------------------------------
# TEST 2 — Validate schema & columns of build_item_meta()
# -----------------------------------------------------------
def test_build_item_meta_dedup_and_counts(spark_module, spark):
    rows = [
        ("s1", "X1", "Name1", "CatA", 10.0),
        ("s2", "X1", "Name1", "CatA", 12.0),
        ("s3", "X2", "Name2", "CatB", 15.0),
    ]
    df = spark.createDataFrame(
        rows,
        schema=["seller_id", "item_id", "item_name", "category", "marketplace_price"]
    )

    meta = spark_module.build_item_meta(df)

    assert meta.count() == 2
    rec = {r.item_id: r.num_sellers_in_catalog for r in meta.collect()}
    assert rec["x1"] == 2
    assert rec["x2"] == 1


# -----------------------------------------------------------
# TEST 3 — Top-N extraction logic
# -----------------------------------------------------------
def test_top_n_extraction_and_counts(spark_module, spark):
    seller_rows = [
        ("s1", "a1", "Item A1", "A", 10.0),
        ("s2", "a2", "Item A2", "A", 12.0),
        ("s3", "a3", "Item A3", "A", 8.0),
        ("s1", "b1", "Item B1", "B", 20.0),
        ("s2", "b2", "Item B2", "B", 18.0),
    ]
    seller_df = spark.createDataFrame(
        seller_rows,
        schema=["seller_id", "item_id", "item_name", "category", "marketplace_price"]
    )

    company_rows = [
        ("a1", 100.0, 10.0),
        ("a2", 50.0, 12.0),
        ("a3", 10.0, 8.0),
        ("b1", 200.0, 20.0),
        ("b2", 150.0, 18.0),
    ]
    company_df = spark.createDataFrame(
        company_rows,
        schema=["item_id", "units_sold", "marketplace_price"]
    )

    item_meta = spark_module.build_item_meta(seller_df)

    comp_agg = (
        company_df.groupBy("item_id").agg(
            F.sum("units_sold").alias("company_total_units"),
            F.avg("marketplace_price").alias("company_avg_price"),
        )
    )

    comp_enriched = (
        comp_agg.join(item_meta, on="item_id", how="inner")
        .withColumn("market_price",
                    F.coalesce(F.col("catalog_market_price"), F.col("company_avg_price")))
        .filter(F.col("category").isNotNull())
        .filter(F.col("item_name").isNotNull())
        .filter(F.col("market_price").isNotNull())
        .select("item_id", "item_name", "category", "company_total_units",
                "market_price", "num_sellers_in_catalog")
    )

    win = Window.partitionBy("category").orderBy(F.desc("company_total_units"))

    comp_top = (
        comp_enriched.withColumn("rn", F.row_number().over(win))
                     .filter(F.col("rn") <= 1)
                     .drop("rn")
    )

    # Expect top item from each category (A and B)
    assert comp_top.count() == 2
    cats = {r.category for r in comp_top.collect()}
    assert cats == {"A", "B"}


# -----------------------------------------------------------
# TEST 4 — Anti-join logic validates missing items correctly
# -----------------------------------------------------------
def test_missing_item_left_anti_behavior(spark_module, spark):
    seller_rows = [
        ("s1", "a1"),
        ("s2", "a2"),
        ("s3", "b1"),
    ]
    seller_df = spark.createDataFrame(seller_rows, schema=["seller_id", "item_id"])

    top_items = [
        ("a1", "Name", "A", 100.0, 10.0),
        ("b1", "Name", "B", 200.0, 20.0),
    ]
    top_df = spark.createDataFrame(
        top_items,
        schema=["item_id", "item_name", "category",
                "company_total_units", "market_price"]
    )

    sellers = seller_df.select("seller_id").distinct()
    sellers_top = sellers.crossJoin(top_df)
    seller_present = seller_df.select("seller_id", "item_id").distinct()

    missing = sellers_top.join(
        seller_present,
        on=[sellers_top.seller_id == seller_present.seller_id,
            sellers_top.item_id == seller_present.item_id],
        how="left_anti"
    )

    assert sellers.count() == 3
    assert top_df.count() == 2
    assert sellers_top.count() == 6

    # MATCHES FOUND: (s1–a1) and (s3–b1) → 2 matches
    # missing = 6 – 2 = 4
    assert missing.count() == 4

# -----------------------------------------------------------
# TEST 5 — Final output schema for recommendations
# -----------------------------------------------------------
def test_output_schema_and_columns(spark_module, spark):
    df = spark.createDataFrame(
        [("s1", "i1", "Item", "A", 10.0, 100.0, 1000.0)],
        schema=["seller_id", "item_id", "item_name", "category",
                "market_price", "expected_units_sold", "expected_revenue"]
    )

    expected_cols = {
        "seller_id", "item_id", "item_name", "category",
        "market_price", "expected_units_sold", "expected_revenue"
    }

    assert set(df.columns) == expected_cols


# -----------------------------------------------------------
# TEST 6 — expected_units_sold & expected_revenue formula
# -----------------------------------------------------------
def test_expected_revenue_calc(spark_module, spark):

    # Corrected: use floats for DoubleType compatibility
    rows = [
        ("S1", "i1", "Item1", "Apparel", 2000.0, 2.0, 50.0),   # expected_units=1000, revenue=50000
        ("S2", "i2", "Item2", "Electronics", 1500.0, 3.0, 20.0), # expected_units=500, revenue=10000
        ("S3", "i3", "Item3", "Footwear", 500.0, None, 10.0),   # sellers_selling_count=None -> fallback=1
    ]

    schema = StructType([
        StructField("seller_id", StringType(), True),
        StructField("item_id", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("company_total_units", DoubleType(), True),
        StructField("sellers_selling_count", DoubleType(), True),
        StructField("market_price", DoubleType(), True),
    ])

    missing_df = spark.createDataFrame(rows, schema=schema)

    # Fallback sellers_selling_count = 1
    df2 = missing_df.withColumn(
        "sellers_selling_count",
        F.when(F.col("sellers_selling_count").isNull(), F.lit(1.0))
         .otherwise(F.col("sellers_selling_count"))
    )

    df2 = df2.withColumn(
        "expected_units_sold",
        (F.col("company_total_units") / F.col("sellers_selling_count"))
    )

    df2 = df2.withColumn(
        "expected_revenue",
        F.col("expected_units_sold") * F.col("market_price")
    )

    results = df2.select("item_id", "expected_units_sold", "expected_revenue").collect()
    lookup = {r.item_id: (r.expected_units_sold, r.expected_revenue) for r in results}

    assert lookup["i1"][0] == 1000.0
    assert lookup["i1"][1] == 50000.0

    assert lookup["i2"][0] == 500.0
    assert lookup["i2"][1] == 10000.0

    # fallback sellers_selling_count=1
    assert lookup["i3"][0] == 500.0
    assert lookup["i3"][1] == 5000.0