# Codebase Structure & Module Reference

## 📂 Directory Organization

```
ecommerce_seller_recommendation/
│
├── s3/                                    # Main S3-based production pipeline
│   │
│   ├── conf/
│   │   └── hudi-defaults.conf            # Hudi table configuration
│   │
│   ├── configs/                          # Configuration files
│   │   ├── ecomm_prod.yml                # Production config (S3 paths)
│   │   ├── ecomm_local.yml               # Local development config
│   │   └── ecommprod.yml                 # Alternative config
│   │
│   ├── data/                             # Sample data & outputs
│   │   ├── dqcheck/                      # Sample dirty data for testing
│   │   │   ├── company_sales_dirty.csv
│   │   │   ├── competitor_sales_dirty.csv
│   │   │   └── seller_catalog_dirty.csv
│   │   │
│   │   └── local/                        # Local execution outputs
│   │       ├── clean/                    # Clean data processing results
│   │       │   ├── input/raw/            # Input CSV files
│   │       │   ├── output/               # Bronze, Silver, Quarantine
│   │       │   └── processed/            # Gold & Recommendations
│   │       │
│   │       └── dirty/                    # Dirty data processing results
│   │           ├── input/raw/
│   │           ├── output/
│   │           └── processed/
│   │
│   ├── libs/                             # Reusable utility modules
│   │   ├── spark_session.py              # Spark initialization & config
│   │   ├── dq.py                         # Data quality validation functions
│   │   ├── log.py                        # Logging utilities
│   │   └── utils.py                      # General utility functions
│   │
│   ├── src/                              # Main ETL & analytics scripts
│   │   ├── etl_company_sales.py          # Company sales ETL pipeline
│   │   ├── etl_competitor_sales.py       # Competitor sales ETL pipeline
│   │   ├── etl_seller_catalog.py         # Seller catalog ETL pipeline
│   │   ├── consumption_recommendation.py # Recommendation engine
│   │   ├── analysis/                     # Analysis & exploration scripts
│   │   └── read_file.py                  # File reading utility
│   │
│   ├── scripts/                          # Bash orchestration scripts
│   │   ├── etl_company_sales_spark_submit.sh
│   │   ├── etl_competitor_sales_spark_submit.sh
│   │   ├── etl_seller_catalog_spark_submit.sh
│   │   ├── consumption_recommendation_spark_submit.sh
│   │   └── analysis/                     # Category-specific scripts
│   │       ├── category/
│   │       └── noncategory/
│   │
│   ├── helper/                           # Helper files & logs
│   │   ├── dqcheck/                      # DQ validation results
│   │   └── logs/                         # Execution logs
│   │       ├── clean/
│   │       │   ├── etl/logs.txt
│   │       │   └── recommendations/logs.txt
│   │       └── dirty/
│   │           ├── etl/logs.txt
│   │           └── recommendations/logs.txt
│   │
│   └── tests/                            # Test & validation scripts
│       ├── test_consumption_recommendations.py
│       ├── validate_expected_units.py
│       ├── validate_logic.py
│       └── analyze_already_owned.py
│
└── local/                                # Local development mirror
    ├── config/
    ├── src/
    ├── libs/
    └── scripts/
```

---

## 🔧 Core Modules

### 1. Spark Session Module (`libs/spark_session.py`)

**Purpose:** Initialize and configure SparkSession with all required dependencies

**Key Functions:**

```python
def get_spark(app_name="ETL") -> Tuple[SparkSession, Dict]:
    """
    Initialize SparkSession with Hudi + S3 + Delta support
    
    Returns:
        Tuple[SparkSession, Dict]: Spark session and loaded config
    """
```

**Configuration Includes:**
- JAR files for Hadoop, AWS, Hudi, Delta
- S3 filesystem configuration
- Kryo serialization for Hudi
- Connection timeout & retry settings
- Adaptive query execution

**Usage:**
```python
from libs.spark_session import get_spark
spark, config = get_spark("MyETLJob")
```

---

### 2. Data Quality Module (`libs/dq.py`)

**Purpose:** Provide reusable DQ validation functions

**Functions:**

| Function | Purpose | Example |
|----------|---------|---------|
| `enforce_not_null()` | Remove null values | `enforce_not_null(df, ['id', 'name'])` |
| `enforce_positive()` | Validate min values | `enforce_positive(df, {'price': 0})` |
| `enforce_max()` | Validate max values | `enforce_max(df, {'qty': 10000})` |
| `enforce_regex()` | Pattern matching | `enforce_regex(df, 'email', r'.*@.*')` |
| `enforce_allowed_values()` | Enum validation | `enforce_allowed_values(df, 'status', ['ACTIVE', 'INACTIVE'])` |
| `dedupe_by_latest()` | Remove duplicates | `dedupe_by_latest(df, ['id'], 'timestamp')` |

**Usage:**
```python
from libs.dq import enforce_not_null, enforce_positive

df = spark.read.csv("data.csv", header=True)
df = enforce_not_null(df, ['item_id', 'seller_id'])
df = enforce_positive(df, {'units_sold': 0, 'revenue': 0})
```

---

### 3. Logging Module (`libs/log.py`)

**Purpose:** Centralized logging configuration

**Functions:**

```python
def get_logger(name: str) -> Logger:
    """Get configured logger instance"""
```

**Usage:**
```python
from libs.log import get_logger

logger = get_logger(__name__)
logger.info("Processing started")
logger.warning("High quarantine rate")
logger.error("Failed to write output")
```

---

### 4. Utilities Module (`libs/utils.py`)

**Purpose:** General utility functions

**Common Functions:**
- Path manipulation
- Date/time utilities
- String normalization
- Configuration helpers

---

## 📊 ETL Scripts

### Company Sales ETL (`src/etl_company_sales.py`)

**Responsibility:** Process company's own sales transactions

**Input:**
- CSV file: `company_sales_dirty.csv`
- Columns: item_id, seller_id, units_sold, revenue, marketplace_price, sale_date

**Processing:**
1. Read raw CSV
2. Write to Bronze (immutable snapshot)
3. Apply DQ rules
4. Deduplicate by (seller_id, item_id)
5. Split valid/invalid records
6. Write Silver (clean data)
7. Write Quarantine (failed records)

**Output:**
- Bronze: `output/bronze/company_sales/run_date=*/`
- Silver: `output/silver/company_sales/`
- Quarantine: `output/quarantine/company_sales/run_date=*/`

**DQ Rules:**
```yaml
Required fields: item_id, seller_id, units_sold, revenue, sale_date
Numeric checks: units_sold >= 0, revenue >= 0, marketplace_price >= 0
Date checks: sale_date <= today, multiple formats supported
Deduplication: Keep first valid (seller_id, item_id) combination
```

**Execution:**
```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  src/etl_company_sales.py \
  --config configs/ecomm_prod.yml
```

---

### Competitor Sales ETL (`src/etl_competitor_sales.py`)

**Responsibility:** Process competitor market data

**Input:**
- CSV file: `competitor_sales_dirty.csv`
- Columns: order_id, customer_id, product_id, order_ts, qty, unit_price

**Processing:**
Similar to company sales but with:
- Different column names (product_id vs item_id)
- Timestamp instead of date
- Customer-level analysis support

**Output:**
- Bronze, Silver, Quarantine (same structure as company sales)

---

### Seller Catalog ETL (`src/etl_seller_catalog.py`)

**Responsibility:** Process seller product catalogs

**Input:**
- CSV file: `seller_catalog_dirty.csv`
- Columns: seller_id, item_id, item_name, category, marketplace_price, stock_qty

**Processing:**
1. Normalize item IDs
2. Validate seller-item combinations
3. Extract category features
4. Validate stock levels
5. Apply DQ rules

**Output:**
- Bronze, Silver, Quarantine (same structure)

---

### Recommendation Engine (`src/consumption_recommendation.py`)

**Responsibility:** Generate item recommendations for sellers

**Algorithm:**

```
1. Load Hudi tables (company, competitor, catalog)
2. Aggregate sales by category & item
3. Identify top-N items per category
4. Create seller × item combinations (cross join)
5. Remove already-owned items (left anti join)
6. Calculate expected units & revenue
7. Write recommendations to CSV
```

**Input:**
- Gold tables: company_sales_hudi, competitor_sales_hudi, seller_catalog_hudi

**Output:**
- `processed/recommendations/company/company_seller_recommendation.csv`
- `processed/recommendations/competitor/competitor_seller_recommendation.csv`

**Output Schema:**
```
seller_id, item_id, item_name, category, market_price, 
expected_units_sold, expected_revenue
```

**Execution:**
```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  src/consumption_recommendation.py \
  --config configs/ecomm_prod.yml
```

---

## 🧪 Test Scripts

### Test Consumption Recommendations (`tests/test_consumption_recommendations.py`)

**Purpose:** Validate recommendation generation logic

**Tests:**
- Recommendation count per seller
- Expected units calculation
- Revenue estimation accuracy
- Missing item identification

---

### Validate Expected Units (`tests/validate_expected_units.py`)

**Purpose:** Verify expected units calculation

**Formula:**
```
expected_units = total_units_sold / number_of_sellers_selling_item
```

**Validation:**
- Check calculation accuracy
- Verify no division by zero
- Validate against historical data

---

### Validate Logic (`tests/validate_logic.py`)

**Purpose:** Verify business logic correctness

**Checks:**
- Top-N selection per category
- Duplicate removal
- Revenue calculations
- Partition correctness

---

### Analyze Already Owned (`tests/analyze_already_owned.py`)

**Purpose:** Analyze seller ownership patterns

**Analysis:**
- Items per seller
- Category distribution
- Overlap between sellers
- Recommendation coverage

---

## ⚙️ Configuration Files

### Production Config (`configs/ecomm_prod.yml`)

**Sections:**

1. **Spark Configuration**
   ```yaml
   spark:
     shuffle_partitions: 8
     log_level: "WARN"
     configs:
       # S3, Delta, Hudi configs
   ```

2. **Data Paths**
   ```yaml
   paths:
     input_root: "s3a://bucket/input/raw"
     output_root: "s3a://bucket/output"
     bronze_*: "s3a://bucket/output/bronze/*"
     silver_*: "s3a://bucket/output/silver/*"
     gold_*: "s3a://bucket/processed/gold/*"
   ```

3. **Data Quality Rules**
   ```yaml
   dq:
     company_sales_required: [item_id, seller_id, ...]
     company_sales_numeric_checks: {...}
     company_sales_date_checks: {...}
   ```

4. **Hudi Configuration**
   ```yaml
   tables:
     company_sales_hudi_table: "company_sales_data"
     competitor_sales_hudi_table: "competitor_sales_data"
     seller_catalog_hudi_table: "seller_catalog_data"
   ```

---

### Hudi Configuration (`conf/hudi-defaults.conf`)

**Key Settings:**
- Table type (COW/MOR)
- Record key field
- Partition path field
- Precombine field
- Compaction settings

---

## 🚀 Orchestration Scripts

### Company Sales Script (`scripts/etl_company_sales_spark_submit.sh`)

**Purpose:** Wrapper script for company sales ETL

**Responsibilities:**
- Set environment variables
- Prepare Spark submit arguments
- Load JAR files
- Execute ETL job
- Capture logs

**Usage:**
```bash
./scripts/etl_company_sales_spark_submit.sh
```

---

### Recommendation Script (`scripts/consumption_recommendation_spark_submit.sh`)

**Purpose:** Wrapper script for recommendation engine

**Responsibilities:**
- Execute recommendation generation
- Handle both company & competitor recommendations
- Manage output partitioning
- Log execution metrics

---

## 📈 Data Flow Summary

```
Raw CSV Files
    ↓
[ETL Scripts]
    ├─→ Bronze (Immutable Snapshot)
    ├─→ DQ Validation
    ├─→ Silver (Clean Data)
    └─→ Quarantine (Failed Records)
    ↓
[Aggregation]
    ├─→ Company Sales Gold
    ├─→ Competitor Sales Gold
    └─→ Seller Catalog Gold
    ↓
[Recommendation Engine]
    ├─→ Top-N Item Selection
    ├─→ Missing Item Identification
    ├─→ Revenue Estimation
    ↓
[Output]
    ├─→ company_seller_recommendation.csv
    └─→ competitor_seller_recommendation.csv
```

---

## 🔍 Module Dependencies

```
spark_session.py
    ├── pyspark
    ├── yaml
    └── os

dq.py
    ├── pyspark.sql.functions
    └── pyspark.sql.window

etl_company_sales.py
    ├── spark_session
    ├── dq
    ├── log
    └── utils

consumption_recommendation.py
    ├── spark_session
    ├── dq
    ├── log
    └── utils
```

---

## 📝 File Naming Conventions

| Pattern | Purpose | Example |
|---------|---------|---------|
| `etl_*.py` | ETL pipeline scripts | `etl_company_sales.py` |
| `*_recommendation.py` | Recommendation logic | `consumption_recommendation.py` |
| `test_*.py` | Unit tests | `test_consumption_recommendations.py` |
| `validate_*.py` | Validation scripts | `validate_logic.py` |
| `*_spark_submit.sh` | Spark submission wrapper | `etl_company_sales_spark_submit.sh` |
| `*.yml` | Configuration files | `ecomm_prod.yml` |
| `*.conf` | Hudi/Spark config | `hudi-defaults.conf` |

---

## 🔐 Security Considerations

### Credential Management
- AWS credentials via IAM role (preferred)
- Environment variables as fallback
- Never hardcode credentials

### Data Access
- S3 bucket policies
- IAM role-based access
- Encryption in transit & at rest

### Audit Trail
- Bronze layer preserves raw data
- Run timestamps for tracking
- Quarantine logs for DQ failures

---

## 📊 Key Metrics & Monitoring

### Execution Metrics
- Total records processed
- Valid records (Silver)
- Quarantine records
- Processing time
- Memory usage

### Data Quality Metrics
- Null value count
- Duplicate count
- Invalid date count
- Out-of-range numeric count

### Recommendation Metrics
- Recommendations per seller
- Average expected revenue
- Category distribution
- Coverage percentage

---

## 🤝 Developer Workflow

### Adding a New ETL Script

1. Create `src/etl_new_source.py`
2. Import from `libs/spark_session`, `libs/dq`
3. Follow existing patterns (Bronze → Silver → Quarantine)
4. Add configuration to `configs/ecomm_prod.yml`
5. Create wrapper script in `scripts/`
6. Add tests in `tests/`

### Adding a New Utility Function

1. Add to appropriate `libs/*.py` file
2. Include docstring with examples
3. Add type hints
4. Update this documentation

### Running Tests

```bash
python tests/test_consumption_recommendations.py
python tests/validate_logic.py
```

---

**Last Updated:** November 2025  
**Version:** 1.0
