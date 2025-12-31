#!/bin/bash
# ==========================================================================================
# Consumption Recommendation- Spark Submit Script (Run from root folder DSP_GA_2025em1100102_20112025)
# ==========================================================================================
#
# Purpose: Execute Consumption Recommendation pipeline using Spark, Hudi, and AWS SDKs.
#          This script runs the Seller Catalog ETL pipeline using Spark + Hudi + AWS SDK.
#
# Overview:
#   This script runs the Consumption Recommendation job which processes cleaned seller catalog, company sales and competitor 
#   sales data from hudi table part of the gold layer and generates top products recommendatinos for sellers.
#
# Overview:
#   The Seller Catalog ETL job:
#     ✔ Reads raw seller catalog data (Hudi snapshot or flat files)
#     ✔ Normalizes item identifiers & attributes
#     ✔ Fixes malformed prices, names, and metadata fields
#     ✔ Applies DQ rules (null checks, numeric fixes, category rules, etc.)
#     ✔ Writes Silver-grade curated seller catalog output
#
# IMPORTANT:
#   ✔ Must be executed from: Root/
#   ✔ Uses only RELATIVE PATHS (same ones used in manual spark-submit execution)
#
# Usage:
#   chmod +x consumption_recommendation_spark_submit.sh
#   ./2025em1100102/ecommerce_seller_recommendation/s3/scripts/consumption_recommendation_spark_submit.sh
#
# Reference
#   (.venv) UIOPYT@HQN473V4C2 DSP_GA_2025em1100102_20112025 % ./2025em1100102/ecommerce_seller_recommendation/s3/scripts/consumption_recommendation_spark_submit.sh
#
# Folder Structure (relative to Root):
#  DSP_GA_2025em1100102_20112025/
#     └── 2025em1100102/
#         └── ecommerce_seller_recommendation/
#             └── s3/
#                 ├── src/consumption_recommendation.py
#                 └── configs/ecomm_prod.yml
#                 └── scripts/consumption_recommendation_spark_submit.sh
#
# Requirements:
#   - Spark installed and available in PATH
#   - Python virtual environment optional (Spark will use system or venv Python)
#   - Internet / S3 access required if S3 paths are used in config
#
# Notes:
#   - You may adjust Spark memory, cores, or serializer settings as needed
#   - This script is suitable for automation (cron, Airflow, Jenkins)
# ==========================================================================================

set -e  # fail on any command error

echo "====================================================="
echo " 🚀 Running Consumption Recommendation via Spark Submit"
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
  2025em1100102/ecommerce_seller_recommendation/s3/src/consumption_recommendation.py \
  --config 2025em1100102/ecommerce_seller_recommendation/s3/configs/ecomm_prod.yml

EXIT_CODE=$?

echo "-----------------------------------------------------"
if [ $EXIT_CODE -eq 0 ]; then
    echo " ✅ Seller Catalog ETL Completed Successfully at $(date)"
else
    echo " ❌ Seller Catalog ETL Failed with exit code: $EXIT_CODE"
fi
echo "====================================================="

exit $EXIT_CODE