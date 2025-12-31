# 🚀 Ecommerce Seller Recommendation System - START HERE

Welcome! This is your entry point to the ecommerce seller recommendation project. This document will guide you through the entire system.

---

## 📚 Documentation Map

Choose your path based on your role:

### 👤 **For New Developers (Start Here)**
1. **Read this file** (you are here)
2. Read `PROJECT_OVERVIEW.md` - Understand what the project does
3. Read `SETUP_GUIDE.md` - Set up your environment
4. Read `QUICK_REFERENCE.md` - Quick commands and troubleshooting
5. Run your first ETL job

### 🏗️ **For Architects & Tech Leads**
1. Read `PROJECT_OVERVIEW.md` - Architecture overview
2. Read `TECHNICAL_GUIDE.md` - Deep dive into implementation
3. Read `CODEBASE_STRUCTURE.md` - Code organization
4. Review `SEQUENCE_DIAGRAM.md` - Data flow visualization

### 👨‍💻 **For Data Engineers**
1. Read `SETUP_GUIDE.md` - Environment setup
2. Read `TECHNICAL_GUIDE.md` - Implementation details
3. Read `CODEBASE_STRUCTURE.md` - Module reference
4. Review ETL scripts in `s3/src/`

### 📊 **For Data Analysts**
1. Read `PROJECT_OVERVIEW.md` - Business context
2. Read `QUICK_REFERENCE.md` - Key metrics and outputs
3. Review test scripts in `s3/tests/`
4. Check output files in `processed/recommendations/`

---

## 🎯 What Does This Project Do?

**In 30 seconds:**
This system analyzes company sales, competitor sales, and seller catalogs to recommend high-performing items that sellers should add to their inventory. It uses Apache Spark and Hudi for large-scale data processing.

**In 2 minutes:**
1. **Ingests** raw sales data from company, competitors, and seller catalogs
2. **Cleans** data using strict quality rules (removes invalid records)
3. **Aggregates** sales by category and item
4. **Recommends** top-selling items missing from each seller's catalog
5. **Estimates** potential revenue for each recommendation

**Business Value:**
- Help sellers discover high-performing items
- Increase seller revenue through data-driven recommendations
- Identify market gaps and opportunities
- Improve inventory decisions

---

## 🏗️ System Architecture (Visual)

```
┌─────────────────────────────────────────────────────────────┐
│                    RAW DATA (CSV)                           │
│  Company Sales | Competitor Sales | Seller Catalogs        │
└────────────────────────┬────────────────────────────────────┘
                         │
                    ┌────▼────┐
                    │  BRONZE  │ (Immutable snapshot)
                    └────┬────┘
                         │
                    ┌────▼────┐
                    │   DQ    │ (Data quality validation)
                    └────┬────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
    ┌───▼──┐        ┌────▼────┐     ┌────▼────┐
    │SILVER│        │QUARANTINE│    │ INVALID │
    │(Clean)       │(Bad Data)│    │ RECORDS │
    └───┬──┘        └──────────┘    └─────────┘
        │
    ┌───▼──────────────────────────┐
    │  GOLD (Aggregates)           │
    │  • Top-N items per category  │
    │  • Sales metrics             │
    └───┬──────────────────────────┘
        │
    ┌───▼──────────────────────────┐
    │  RECOMMENDATIONS             │
    │  • Missing items per seller  │
    │  • Expected revenue          │
    └──────────────────────────────┘
```

---

## 📂 Project Structure (Simplified)

```
ecommerce_seller_recommendation/
├── s3/                          # Main pipeline
│   ├── src/                     # ETL & recommendation scripts
│   │   ├── etl_company_sales.py
│   │   ├── etl_competitor_sales.py
│   │   ├── etl_seller_catalog.py
│   │   └── consumption_recommendation.py
│   │
│   ├── libs/                    # Reusable utilities
│   │   ├── spark_session.py     # Spark setup
│   │   ├── dq.py                # Data quality functions
│   │   └── utils.py
│   │
│   ├── configs/                 # Configuration
│   │   └── ecomm_prod.yml       # Main config (UPDATE THIS!)
│   │
│   ├── scripts/                 # Bash wrappers
│   │   ├── etl_company_sales_spark_submit.sh
│   │   ├── etl_competitor_sales_spark_submit.sh
│   │   ├── etl_seller_catalog_spark_submit.sh
│   │   └── consumption_recommendation_spark_submit.sh
│   │
│   ├── tests/                   # Validation scripts
│   │   ├── test_consumption_recommendations.py
│   │   └── validate_logic.py
│   │
│   └── data/                    # Sample data
│       └── dqcheck/             # Test data files
│
└── local/                       # Local development mirror
```

---

## ⚡ Quick Start (10 Minutes)

### Step 1: Check Prerequisites
```bash
python3 --version        # Should be 3.8+
java -version            # Should be 11+
spark-submit --version   # Should be 3.5.2
aws s3 ls                # Should list your buckets
```

### Step 2: Setup Environment
```bash
cd ecommerce_seller_recommendation/

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install pyspark==3.5.2 delta-spark==3.2.0 pyarrow pyyaml s3fs pandas
```

### Step 3: Configure Project
```bash
# Edit configuration file
nano ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/configs/ecomm_prod.yml

# Update these values:
# - S3 bucket name (if different)
# - JAR paths (if different)
```

### Step 4: Run ETL Pipeline
```bash
# Run company sales ETL
./ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/scripts/etl_company_sales_spark_submit.sh

# Run competitor sales ETL
./ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/scripts/etl_competitor_sales_spark_submit.sh

# Run seller catalog ETL
./ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/scripts/etl_seller_catalog_spark_submit.sh

# Generate recommendations
./ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/scripts/consumption_recommendation_spark_submit.sh
```

### Step 5: Check Results
```bash
# List recommendations
aws s3 ls s3://ecommerce_seller_recommendation/ecommerce_seller_recommendation/processed/recommendations/

# Download a sample
aws s3 cp s3://ecommerce_seller_recommendation/ecommerce_seller_recommendation/processed/recommendations/company/company_seller_recommendation.csv ./

# View first few rows
head -20 company_seller_recommendation.csv
```

---

## 🔄 Data Flow Explained

### Input Data
```
company_sales_dirty.csv
├── item_id, seller_id, units_sold, revenue, marketplace_price, sale_date
└── ~1,000,000 rows (may contain errors)

competitor_sales_dirty.csv
├── order_id, customer_id, product_id, order_ts, qty, unit_price
└── ~1,000,000 rows (may contain errors)

seller_catalog_dirty.csv
├── seller_id, item_id, item_name, category, marketplace_price, stock_qty
└── ~1,000,500 rows (may contain errors)
```

### Processing Steps
1. **Bronze Layer** - Save raw data as-is (audit trail)
2. **Data Quality** - Apply validation rules:
   - Check required fields are not null
   - Validate numeric ranges
   - Parse dates correctly
   - Remove duplicates
3. **Silver Layer** - Save clean, valid data
4. **Quarantine** - Save invalid records (for review)
5. **Gold Layer** - Aggregate and create features
6. **Recommendations** - Generate seller recommendations

### Output Data
```
company_seller_recommendation.csv
├── seller_id, item_id, item_name, category
├── market_price, expected_units_sold, expected_revenue
└── One row per (seller, recommended_item) pair

competitor_seller_recommendation.csv
├── Same structure as above
└── Recommendations from competitor data
```

---

## 📊 Key Metrics

| Metric | Expected Value | Location |
|--------|---|---|
| **Total Input Records** | ~1,000,000 | `input/raw/` |
| **Valid Records (Silver)** | ~903,696 | `output/silver/` |
| **Invalid Records (Quarantine)** | ~96,304 | `output/quarantine/` |
| **Recommendations Generated** | Varies | `processed/recommendations/` |
| **Processing Time** | 5-15 min | Spark logs |

---

## 🧪 Validation

### Run Tests
```bash
# Test recommendation logic
python ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/tests/test_consumption_recommendations.py

# Validate business logic
python ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/tests/validate_logic.py

# Check expected units calculation
python ecommerce_seller_recommendation/ecommerce_seller_recommendation/s3/tests/validate_expected_units.py
```

### Check Logs
```bash
# View ETL logs
cat ecommerce_seller_recommendation/ecommerce_seller_recommendation/helper/logs/clean/etl/logs.txt

# View recommendation logs
cat ecommerce_seller_recommendation/ecommerce_seller_recommendation/helper/logs/clean/recommendations/logs.txt
```

### Inspect Quarantine Files
```bash
# Download quarantine records
aws s3 cp s3://bucket/output/quarantine/company_sales/ . --recursive

# View invalid records
head -100 company_sales_quarantine.csv
```

---

## 🐛 Troubleshooting

### Issue: "S3 connection timeout"
**Solution:** Increase timeout in `ecomm_prod.yml`:
```yaml
spark:
  configs:
    spark.hadoop.fs.s3a.connection.timeout: "120000"  # 2 minutes
```

### Issue: "Out of memory"
**Solution:** Reduce shuffle partitions or increase memory:
```yaml
spark:
  shuffle_partitions: 4  # Reduce from 8
```

### Issue: "AWS credentials not found"
**Solution:** Configure AWS:
```bash
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key
# Enter default region (e.g., ap-south-1)
```

### Issue: "Hudi table not found"
**Solution:** Verify S3 paths in config match actual structure:
```bash
aws s3 ls s3://ecommerce_seller_recommendation/ecommerce_seller_recommendation/
```

---

## 📖 Documentation Guide

| Document | Read When | Time |
|----------|-----------|------|
| **PROJECT_OVERVIEW.md** | You want to understand the system | 15 min |
| **SETUP_GUIDE.md** | You need to install/configure | 20 min |
| **QUICK_REFERENCE.md** | You need quick commands | 5 min |
| **TECHNICAL_GUIDE.md** | You want implementation details | 30 min |
| **CODEBASE_STRUCTURE.md** | You want to understand code organization | 20 min |
| **SEQUENCE_DIAGRAM.md** | You want to see data flow | 5 min |

---

## 🎯 Common Tasks

### Task: Run ETL for the first time
1. Follow "Quick Start" section above
2. Check outputs in S3
3. Review logs for errors

### Task: Add a new data source
1. Create new ETL script in `s3/src/`
2. Follow existing patterns (Bronze → Silver → Quarantine)
3. Add configuration to `ecomm_prod.yml`
4. Create wrapper script in `s3/scripts/`

### Task: Modify DQ rules
1. Edit `ecomm_prod.yml` under `dq:` section
2. Update validation logic in `s3/libs/dq.py` if needed
3. Run tests to verify changes
4. Re-run ETL pipeline

### Task: Debug a failed job
1. Check logs in `helper/logs/`
2. Review quarantine files for invalid records
3. Verify input data format
4. Check S3 paths and permissions
5. Run test scripts

### Task: Optimize performance
1. Increase `shuffle_partitions` for large datasets
2. Increase Spark memory allocation
3. Review Spark UI at `http://localhost:4040`
4. Check for skewed partitions

---

## 🔐 Security Notes

- **AWS Credentials:** Use IAM roles when possible, not hardcoded keys
- **Data Privacy:** Quarantine layer isolates sensitive/invalid data
- **Audit Trail:** Bronze layer preserves raw data for compliance
- **Encryption:** S3 supports encryption in transit and at rest

---

## 💡 Pro Tips

1. **Local Development:** Use `ecomm_local.yml` with local file paths
2. **Debugging:** Set `log_level: "INFO"` for detailed logs
3. **Testing:** Always run validation scripts before production
4. **Monitoring:** Check Spark UI while jobs are running
5. **Backups:** Keep quarantine files for audit trails
6. **Performance:** Start with small data, scale up gradually

---

## 🤝 Getting Help

### If something doesn't work:
1. Check `QUICK_REFERENCE.md` for common issues
2. Review logs in `helper/logs/`
3. Inspect quarantine files for data issues
4. Verify configuration in `ecomm_prod.yml`
5. Run test scripts to validate setup
6. Check AWS credentials and S3 access

### If you need more details:
1. Read the relevant documentation file
2. Review the source code with comments
3. Check test scripts for usage examples
4. Review TECHNICAL_GUIDE.md for deep dives

---

## 📞 Quick Links

- **Setup Instructions:** See `SETUP_GUIDE.md`
- **Architecture Details:** See `PROJECT_OVERVIEW.md`
- **Code Reference:** See `TECHNICAL_GUIDE.md`
- **File Organization:** See `CODEBASE_STRUCTURE.md`
- **Quick Commands:** See `QUICK_REFERENCE.md`
- **Data Flow:** See `SEQUENCE_DIAGRAM.md`

---

## ✅ Your Next Steps

1. **Right Now:**
   - Read this file (you're doing it!)
   - Check prerequisites

2. **Next (5-10 minutes):**
   - Follow "Quick Start" section
   - Run first ETL job

3. **Then:**
   - Check outputs
   - Review logs
   - Run validation tests

4. **Finally:**
   - Read full documentation
   - Explore the codebase
   - Customize for your needs

---

## 📝 Project Info

- **Project ID:** 2025em1100102
- **Created:** November 2025
- **Technology:** Apache Spark 3.5.2, Hudi 0.15.0
- **Language:** Python 3.8+
- **Data Source:** AWS S3
- **Architecture:** Medallion (Bronze → Silver → Gold)

---

## 🎓 Learning Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Hudi Documentation](https://hudi.apache.org/)
- [AWS S3 Best Practices](https://docs.aws.amazon.com/s3/)
- [Medallion Architecture Pattern](https://www.databricks.com/blog/2022/06/24/use-the-medallion-architecture-to-build-data-pipelines.html)

---

**Ready to get started? Follow the Quick Start section above! 🚀**

For detailed information, see the other documentation files listed in the "Documentation Map" section.