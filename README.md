# Ecommerce Seller Recommendation System - Project Overview

## 📋 Executive Summary

This is a **data engineering and analytics project** that builds a recommendation engine for ecommerce sellers. The system analyzes company sales, competitor sales, and seller catalogs to recommend high-performing items that sellers should add to their inventory.

**Key Objectives:**
- Identify top-selling items within the company and competitor markets
- Recommend missing items to sellers based on market performance
- Estimate potential revenue for recommended items
- Process and clean large-scale transactional data using Apache Spark and Hudi

---

## 🏗️ Architectural Overview

### Data Architecture (Medallion Pattern)

The project follows the **Medallion Architecture** (Bronze → Silver → Gold):

```
┌─────────────────────────────────────────────────────────────────┐
│                    RAW DATA SOURCES (CSV)                       │
│  • company_sales_dirty.csv                                      │
│  • competitor_sales_dirty.csv                                   │
│  • seller_catalog_dirty.csv                                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  BRONZE LAYER   │
                    │  (Raw Snapshot) │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  DQ & CLEANING  │
                    │  (Validation)   │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
   ┌────▼────┐        ┌─────▼──────┐      ┌──────▼────┐
   │ SILVER   │        │ QUARANTINE │      │ INVALID   │
   │ (Clean)  │        │ (Bad Data) │      │ RECORDS   │
   └────┬────┘        └────────────┘      └───────────┘
        │
   ┌────▼──────────────────────────────────────┐
   │  GOLD LAYER (Aggregates & Features)       │
   │  • Company Sales Aggregates               │
   │  • Competitor Sales Aggregates            │
   │  • Seller Catalog Features                │
   └────┬──────────────────────────────────────┘
        │
   ┌────▼──────────────────────────────────────┐
   │  RECOMMENDATION ENGINE                    │
   │  • Top-N Item Selection                   │
   │  • Missing Item Identification            │
   │  • Revenue Estimation                     │
   └────┬──────────────────────────────────────┘
        │
   ┌────▼──────────────────────────────────────┐
   │  OUTPUT (CSV Recommendations)             │
   │  • company_seller_recommendation.csv      │
   │  • competitor_seller_recommendation.csv   │
   └───────────────────────────────────────────┘
```

### Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Compute Engine** | Apache Spark | 3.5.2 |
| **Data Format** | Apache Hudi | 0.15.0 |
| **Data Lake** | AWS S3 | s3a:// |
| **Language** | Python | 3.8+ |
| **Config Management** | YAML | - |
| **Serialization** | Kryo | - |

---

## 📁 Project Structure

```
ecommerce_seller_recommendation/
├── s3/                              # Main S3-based ETL pipeline
│   ├── conf/
│   │   └── hudi-defaults.conf       # Hudi configuration
│   │
│   ├── configs/
│   │   ├── ecomm_prod.yml           # Production config (S3 paths)
│   │   ├── ecomm_local.yml          # Local development config
│   │   └── ecommprod.yml            # Alternative prod config
│   │
│   ├── data/
│   │   ├── dqcheck/                 # Sample dirty data for testing
│   │   │   ├── company_sales_dirty.csv
│   │   │   ├── competitor_sales_dirty.csv
│   │   │   └── seller_catalog_dirty.csv
│   │   │
│   │   └── local/                   # Local execution outputs
│   │       ├── clean/               # Clean data processing
│   │       │   ├── input/raw/       # Input CSVs
│   │       │   ├── output/          # Bronze, Silver, Quarantine
│   │       │   └── processed/       # Gold & Recommendations
│   │       │
│   │       └── dirty/               # Dirty data processing
│   │           ├── input/raw/
│   │           ├── output/
│   │           └── processed/
│   │
│   ├── libs/                        # Reusable utility modules
│   │   ├── spark_session.py         # Spark initialization
│   │   ├── dq.py                    # Data quality functions
│   │   ├── log.py                   # Logging utilities
│   │   └── utils.py                 # General utilities
│   │
│   ├── src/                         # Main ETL & Recommendation scripts
│   │   ├── etl_company_sales.py     # Company sales ETL
│   │   ├── etl_competitor_sales.py  # Competitor sales ETL
│   │   ├── etl_seller_catalog.py    # Seller catalog ETL
│   │   ├── consumption_recommendation.py  # Recommendation engine
│   │   ├── analysis/                # Analysis scripts
│   │   └── read_file.py             # File reading utility
│   │
│   ├── scripts/                     # Bash orchestration scripts
│   │   ├── etl_company_sales_spark_submit.sh
│   │   ├── etl_competitor_sales_spark_submit.sh
│   │   ├── etl_seller_catalog_spark_submit.sh
│   │   └── consumption_recommendation_spark_submit.sh
│   │
│   ├── helper/                      # Helper files & logs
│   │   ├── dqcheck/                 # DQ validation results
│   │   └── logs/                    # Execution logs
│   │
│   └── tests/                       # Test & validation scripts
│       ├── test_consumption_recommendations.py
│       ├── validate_expected_units.py
│       ├── validate_logic.py
│       └── analyze_already_owned.py
│
└── local/                           # Local development setup (mirror of s3/)
    ├── config/
    ├── src/
    ├── libs/
    └── scripts/
```

---

## 🔄 End-to-End Workflow

### 1. **Data Ingestion & Bronze Layer**
```
Raw CSV Files (S3 or Local)
    ↓
[etl_company_sales.py]
[etl_competitor_sales.py]
[etl_seller_catalog.py]
    ↓
Bronze Layer (Immutable snapshot)
- Partitioned by run_date
- Preserves all raw data
```

**Input Files:**
- `company_sales_dirty.csv` - Company's own sales transactions
- `competitor_sales_dirty.csv` - Competitor sales data
- `seller_catalog_dirty.csv` - Seller product catalogs

**Output:**
- Bronze tables with timestamp partitions

---

### 2. **Data Quality & Cleaning (Silver Layer)**

Each ETL script applies strict DQ rules:

#### **Company Sales DQ Rules:**
- ✅ `item_id` must NOT be null/empty
- ✅ `seller_id` must NOT be null/empty
- ✅ `units_sold` must be numeric and ≥ 0
- ✅ `revenue` must be numeric and ≥ 0
- ✅ `marketplace_price` must be numeric and ≥ 0
- ✅ `sale_date` must parse successfully and be ≤ today
- ✅ Duplicates removed based on `(seller_id + item_id)` composite key
- ✅ Zero values ARE allowed for numeric fields

#### **Invalid Records Handling:**
- Records failing ANY rule → **Quarantine** (with reason)
- Valid records → **Silver** (Hudi table)

**Output:**
- Silver tables (Hudi format) - Clean, deduplicated data
- Quarantine tables - Failed records with reasons

---

### 3. **Aggregation & Feature Engineering (Gold Layer)**

```python
# Company Sales Aggregation
- Group by: category, item_id
- Metrics: total_units_sold, total_revenue, avg_price
- Output: company_sales_gold

# Competitor Sales Aggregation
- Group by: category, item_id
- Metrics: total_units_sold, total_revenue, avg_price
- Output: competitor_sales_gold

# Seller Catalog Features
- Normalize item IDs
- Extract category information
- Output: seller_catalog_gold
```

---

### 4. **Recommendation Engine**

**Company Recommendations:**
1. Identify top-N items per category (from company sales)
2. For each seller, find missing items from top-N
3. Calculate expected revenue:
   - `expected_units = total_units_sold / num_sellers_selling_item`
   - `expected_revenue = expected_units × marketplace_price`

**Competitor Recommendations:**
1. Identify top-N items per category (from competitor sales)
2. Find items missing from company catalog
3. Estimate market potential and expected revenue

**Output Files:**
- `company_seller_recommendation.csv` - Per-seller recommendations from company data
- `competitor_seller_recommendation.csv` - Per-seller recommendations from competitor data

**Columns:**
```
seller_id, item_id, item_name, category, market_price, 
expected_units_sold, expected_revenue
```

---

## 🚀 Quick Start Guide

### Prerequisites
- Python 3.8+
- Apache Spark 3.5.2
- AWS credentials configured (for S3 access)
- Java 11+

### Step 1: Environment Setup

```bash
# Navigate to project root
cd ecommerce_seller_recommendation/

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install pyspark==3.5.2
pip install delta-spark==3.2.0
pip install pyarrow
pip install pyyaml
pip install s3fs
pip install pandas
```

### Step 2: Configure Paths

Edit `s3/configs/ecomm_prod.yml`:
- Update S3 bucket paths if using different bucket
- Adjust Spark configurations for your cluster
- Set DQ rules as needed

### Step 3: Run ETL Pipeline

```bash
# Company Sales ETL
./ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/scripts/etl_company_sales_spark_submit.sh

# Competitor Sales ETL
./ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/scripts/etl_competitor_sales_spark_submit.sh

# Seller Catalog ETL
./ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/scripts/etl_seller_catalog_spark_submit.sh

# Generate Recommendations
./ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/scripts/consumption_recommendation_spark_submit.sh
```

### Step 4: Verify Outputs

Check output directories:
```bash
# Bronze (raw snapshots)
s3a://ecommerce_seller_recommendation/ecommerce_seller_recommendation/output/bronze/

# Silver (cleaned data)
s3a://ecommerce_seller_recommendation/ecommerce_seller_recommendation/output/silver/

# Gold (aggregates)
s3a://ecommerce_seller_recommendation/ecommerce_seller_recommendation/processed/gold/

# Recommendations
s3a://ecommerce_seller_recommendation/ecommerce_seller_recommendation/processed/recommendations/
```

---

## 📊 Data Flow Details

### Input Data Schemas

#### **Company Sales**
```
item_id (string)
seller_id (string)
units_sold (int)
revenue (double)
marketplace_price (double)
sale_date (date)
```

#### **Competitor Sales**
```
order_id (string)
customer_id (string)
product_id (string)
order_ts (timestamp)
qty (int)
unit_price (double)
```

#### **Seller Catalog**
```
seller_id (string)
item_id (string)
item_name (string)
category (string)
marketplace_price (double)
stock_qty (int)
```

### Output Data Schemas

#### **Recommendations**
```
seller_id (string)
item_id (string)
item_name (string)
category (string)
market_price (double)
expected_units_sold (double)
expected_revenue (double)
```

---

## 🔧 Configuration Management

### Key Configuration Files

**`ecomm_prod.yml`** - Production configuration
- Spark settings (shuffle partitions, log level)
- S3 paths for all data layers
- DQ rules and validation thresholds
- Hudi table names

**`hudi-defaults.conf`** - Hudi-specific settings
- Table format (COW/MOR)
- Commit metadata
- Compaction policies

### Environment Variables

```bash
# AWS Credentials (if not using IAM role)
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>

# Spark settings
export SPARK_HOME=/path/to/spark
export JAVA_HOME=/path/to/java
```

---

## 📈 Key Metrics & Validation

### Data Quality Metrics

| Metric | Expected | Actual |
|--------|----------|--------|
| Total Bronze Records | Raw input | ~1,000,000 |
| Valid Silver Records | After DQ | ~903,696 |
| Quarantine Records | Failed DQ | ~96,304 |
| Duplicate Records | Removed | ~19,896 |

### Performance Metrics

- **Processing Time:** Depends on data volume and cluster size
- **Partitions:** Configured via `spark.sql.shuffle.partitions`
- **Memory:** Adjust via Spark executor/driver settings

---

## 🧪 Testing & Validation

### Test Scripts

Located in `s3/tests/`:

- **`test_consumption_recommendations.py`** - Validate recommendation logic
- **`validate_expected_units.py`** - Check expected units calculation
- **`validate_logic.py`** - Verify business logic
- **`analyze_already_owned.py`** - Analyze seller ownership patterns

### Running Tests

```bash
# Test recommendation generation
python ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/tests/test_consumption_recommendations.py

# Validate business logic
python ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/tests/validate_logic.py
```

---

## 📝 Logging & Monitoring

### Log Locations

```
helper/logs/
├── clean/
│   ├── etl/logs.txt              # ETL execution logs
│   └── recommendations/logs.txt  # Recommendation logs
└── dirty/
    ├── etl/logs.txt
    └── recommendations/logs.txt
```

### Log Levels

- **WARN** - Default (warnings and errors)
- **INFO** - Detailed execution flow
- **DEBUG** - Variable states and intermediate results

---

## 🔐 Security & Best Practices

### AWS S3 Access
- Uses `DefaultAWSCredentialsProviderChain` (IAM role preferred)
- Supports environment variable credentials as fallback
- S3A filesystem configured with retry logic

### Data Privacy
- Quarantine layer isolates invalid/sensitive data
- Run timestamps for audit trails
- Partition pruning for efficient queries

### Error Handling
- DQ failures logged with reasons
- Duplicate detection prevents data inconsistencies
- Retry logic for transient S3 failures

---

## 🤝 For New Developers

### Getting Started Checklist

- [ ] Clone/access the repository
- [ ] Set up Python virtual environment
- [ ] Install dependencies (see Quick Start)
- [ ] Configure AWS credentials
- [ ] Update `ecomm_prod.yml` with your S3 paths
- [ ] Run a single ETL script to verify setup
- [ ] Check output in S3 or local directories
- [ ] Review logs for any errors
- [ ] Run recommendation engine
- [ ] Validate output CSVs

### Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| S3 connection timeout | Increase `fs.s3a.connection.timeout` in config |
| Out of memory | Increase Spark executor memory or reduce shuffle partitions |
| Hudi table not found | Verify paths in config match actual S3 structure |
| DQ failures | Check quarantine files for invalid records |
| Duplicate key errors | Verify composite key logic in DQ rules |

---

## 📚 Additional Resources

- **Spark Documentation:** https://spark.apache.org/docs/latest/
- **Hudi Documentation:** https://hudi.apache.org/
- **AWS S3 Best Practices:** https://docs.aws.amazon.com/s3/latest/userguide/
- **Medallion Architecture:** https://www.databricks.com/blog/2022/06/24/use-the-medallion-architecture-to-build-data-pipelines.html

---

## 📞 Support & Contribution

For issues or questions:
1. Check logs in `helper/logs/`
2. Review DQ rules in `ecomm_prod.yml`
3. Validate input data format
4. Check S3 paths and permissions
5. Review test scripts for expected behavior