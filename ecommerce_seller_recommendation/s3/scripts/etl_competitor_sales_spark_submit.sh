#!/bin/bash
# ==========================================================================================
# Competitor Sales ETL - Spark Submit Script (Run from root folder DSP_GA_2025em1100102_20112025)
# ==========================================================================================
# 
# Purpose: Execute the Competitor Sales ETL pipeline using Spark, Hudi, and AWS SDKs.
#          This script runs the Competitor Sales ETL pipeline using Spark + Hudi + AWS SDK.
#
# Overview:
#   This script runs the ETL job that processes competitor sales data stored in Hudi tables.
#   It generates cleansed Silver data, performs DQ validation, derives computed fields,
#   and writes outputs back to configured destinations (local / S3 / Hudi tables).
#
# Overview:
#   The Competitor Sales ETL job:
#     ✔ Reads raw competitor sales data (Hudi snapshot or flat files)
#     ✔ Normalizes item identifiers & attributes
#     ✔ Fixes malformed prices, names, and metadata fields
#     ✔ Applies DQ rules (null checks, numeric fixes, category rules, etc.)
#     ✔ Writes Silver-grade curated seller catalog output
#
# IMPORTANT:
#   ✔ Must be executed from: Root folder DSP_GA_2025em1100102_20112025/
#   ✔ Uses only RELATIVE PATHS (same paths that work in your manual command)
#
# Usage:
#   chmod +x etl_seller_catalog_spark_submit.sh
#   ./2025em1100102/ecommerce_seller_recommendation/s3/scripts/etl_competitor_sales_spark_submit.sh
#
# Reference
#   (.venv) UIOPYT@HQN473V4C2 DSP_GA_2025em1100102_20112025 % ./2025em1100102/ecommerce_seller_recommendation/s3/scripts/etl_competitor_sales_spark_submit.sh
#
# Folder Structure (relative to Root):
#  DSP_GA_2025em1100102_20112025/
#     └── 2025em1100102/
#         └── ecommerce_seller_recommendation/
#             └── s3/
#                 ├── src/etl_competitor_sales.py
#                 └── configs/ecomm_prod.yml
#                 └── scripts/etl_competitor_sales_spark_submit.sh
#
# Requirements:
#   - Spark must be installed and available in PATH
#   - Python virtual environment optional (not auto-activated)
#   - Network access required if S3 paths are used
#
# Notes:
#   - You may modify Spark configs as needed (memory, cores, serializer etc.)
#   - This script can be used inside automation (cron, Airflow, Jenkins)
# ==========================================================================================

set -e  # fail on first error

echo "====================================================="
echo " 🚀 Running Competitor Sales ETL via Spark Submit"
echo "====================================================="
echo "Timestamp: $(date)"
echo "PWD: $(pwd)"
echo "-----------------------------------------------------"

# -----------------------------
# Execute Spark Submit
# -----------------------------
spark-submit \
  --packages "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  2025em1100102/ecommerce_seller_recommendation/s3/src/etl_competitor_sales.py \
  --config 2025em1100102/ecommerce_seller_recommendation/s3/configs/ecomm_prod.yml

EXIT_CODE=$?

echo "-----------------------------------------------------"
if [ $EXIT_CODE -eq 0 ]; then
    echo " ✅ ETL Completed Successfully at $(date)"
else
    echo " ❌ ETL Failed with exit code: $EXIT_CODE"
fi
echo "====================================================="

exit $EXIT_CODE