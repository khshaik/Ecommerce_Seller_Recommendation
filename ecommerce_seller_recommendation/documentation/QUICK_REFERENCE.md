# Quick Reference Guide

## 🚀 Quick Start (5 Minutes)

### Prerequisites Check
```bash
python3 --version          # Python 3.8+
java -version              # Java 11+
spark-submit --version     # Spark 3.5.2
aws s3 ls                  # AWS credentials configured
```

### Setup
```bash
cd DSP_GA_2025em1100102_20112025/
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark==3.5.2 delta-spark==3.2.0 pyarrow pyyaml s3fs pandas
```

### Run Pipeline
```bash
# Company Sales ETL
./2025em1100102/ecommerce_seller_recommendation/s3/scripts/etl_company_sales_spark_submit.sh

# Competitor Sales ETL
./2025em1100102/ecommerce_seller_recommendation/s3/scripts/etl_competitor_sales_spark_submit.sh

# Seller Catalog ETL
./2025em1100102/ecommerce_seller_recommendation/s3/scripts/etl_seller_catalog_spark_submit.sh

# Generate Recommendations
./2025em1100102/ecommerce_seller_recommendation/s3/scripts/consumption_recommendation_spark_submit.sh
```

### Verify Output
```bash
aws s3 ls s3://2025em1100102/dsp_ga_2025em1100102_20112025/processed/recommendations/
```

---

## 📁 Key File Locations

| File | Purpose | Location |
|------|---------|----------|
| **Configuration** | Production config | `s3/configs/ecomm_prod.yml` |
| **Company Sales ETL** | Process company data | `s3/src/etl_company_sales.py` |
| **Competitor Sales ETL** | Process competitor data | `s3/src/etl_competitor_sales.py` |
| **Seller Catalog ETL** | Process catalog data | `s3/src/etl_seller_catalog.py` |
| **Recommendations** | Generate recommendations | `s3/src/consumption_recommendation.py` |
| **DQ Functions** | Data quality utilities | `s3/libs/dq.py` |
| **Spark Setup** | Spark initialization | `s3/libs/spark_session.py` |
| **Tests** | Validation scripts | `s3/tests/` |

---

## 🔄 Data Flow

```
Raw CSV
  ↓
Bronze (Snapshot)
  ↓
DQ Validation
  ↓
Silver (Clean) + Quarantine (Invalid)
  ↓
Gold (Aggregates)
  ↓
Recommendations
```

---

## 📊 Data Schemas

### Company Sales Input
```
item_id (string)
seller_id (string)
units_sold (int)
revenue (double)
marketplace_price (double)
sale_date (string)
```

### Recommendations Output
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

## ⚙️ Configuration Quick Edit

Edit `s3/configs/ecomm_prod.yml`:

```yaml
# Update S3 bucket name
paths:
  input_root: "s3a://YOUR-BUCKET/dsp_ga_2025em1100102_20112025/input/raw"
  output_root: "s3a://YOUR-BUCKET/dsp_ga_2025em1100102_20112025/output"

# Adjust for your cluster
spark:
  shuffle_partitions: 8  # Increase for larger clusters
```

---

## 🧪 Run Tests

```bash
# Test recommendations
python 2025em1100102/ecommerce_seller_recommendation/s3/tests/test_consumption_recommendations.py

# Validate logic
python 2025em1100102/ecommerce_seller_recommendation/s3/tests/validate_logic.py

# Check expected units
python 2025em1100102/ecommerce_seller_recommendation/s3/tests/validate_expected_units.py
```

---

## 📈 Key Metrics

| Metric | Expected | Location |
|--------|----------|----------|
| Total Bronze Records | ~1,000,000 | `output/bronze/` |
| Valid Silver Records | ~903,696 | `output/silver/` |
| Quarantine Records | ~96,304 | `output/quarantine/` |
| Recommendations | Per seller | `processed/recommendations/` |

---

## 🔐 DQ Rules Summary

### Company Sales
- ✅ item_id, seller_id NOT NULL
- ✅ units_sold, revenue, marketplace_price ≥ 0
- ✅ sale_date ≤ today
- ✅ Deduplicate by (seller_id, item_id)

### Competitor Sales
- ✅ order_id, customer_id, product_id NOT NULL
- ✅ qty, unit_price ≥ 0
- ✅ order_ts valid timestamp

### Seller Catalog
- ✅ seller_id, item_id NOT NULL
- ✅ marketplace_price, stock_qty ≥ 0

---

## 🐛 Common Issues

| Issue | Solution |
|-------|----------|
| S3 timeout | Increase `fs.s3a.connection.timeout` in config |
| Out of memory | Reduce `shuffle_partitions` or increase executor memory |
| Hudi table not found | Verify S3 paths in config match actual structure |
| DQ failures | Check `output/quarantine/` for invalid records |
| AWS credentials error | Run `aws configure` or set env vars |

---

## 📚 Documentation Files

| File | Content |
|------|---------|
| `PROJECT_OVERVIEW.md` | Architecture, workflow, quick start |
| `SETUP_GUIDE.md` | Installation, configuration, troubleshooting |
| `TECHNICAL_GUIDE.md` | Implementation details, code examples |
| `CODEBASE_STRUCTURE.md` | Module reference, file organization |
| `SEQUENCE_DIAGRAM.md` | ETL sequence flow |
| `ARCHITECTURE_WORKFLOW.svg` | Visual architecture diagram |

---

## 🔗 Useful Commands

```bash
# List S3 contents
aws s3 ls s3://bucket-name/ --recursive

# Copy from S3
aws s3 cp s3://bucket/path/file.csv ./local-file.csv

# Monitor Spark job
# Open http://localhost:4040 while job running

# Check Spark logs
tail -f /path/to/spark/logs/spark-*.log

# Kill Spark job
pkill -f spark-submit

# Test S3 access
python -c "import boto3; s3=boto3.client('s3'); print(s3.list_buckets())"
```

---

## 📞 Getting Help

1. **Check logs:** `helper/logs/clean/etl/logs.txt`
2. **Review DQ failures:** `output/quarantine/`
3. **Validate input:** Check CSV format matches schema
4. **Test S3 access:** `aws s3 ls s3://bucket-name/`
5. **Check config:** Verify paths in `ecomm_prod.yml`
6. **Review tests:** Run validation scripts in `tests/`

---

## 🎯 Next Steps After Setup

1. ✅ Verify all prerequisites installed
2. ✅ Configure AWS credentials
3. ✅ Update `ecomm_prod.yml` with your S3 paths
4. ✅ Run company sales ETL
5. ✅ Check output in S3
6. ✅ Run all ETL scripts
7. ✅ Generate recommendations
8. ✅ Validate output CSVs
9. ✅ Run test scripts
10. ✅ Review metrics and logs

---

## 💡 Pro Tips

- **Local Development:** Use `ecomm_local.yml` with local file paths
- **Debugging:** Set `log_level: "INFO"` in config for detailed logs
- **Performance:** Increase `shuffle_partitions` for larger datasets
- **Monitoring:** Check Spark UI at `http://localhost:4040`
- **Testing:** Run validation scripts before production runs
- **Backups:** Keep quarantine files for audit trails

---

## 🔄 Typical Workflow

```
1. Setup Environment
   └─ Install dependencies, configure AWS

2. Configure Project
   └─ Update ecomm_prod.yml with S3 paths

3. Run ETL Pipeline
   └─ Execute company, competitor, catalog ETLs

4. Monitor Execution
   └─ Check logs, verify output in S3

5. Generate Recommendations
   └─ Run consumption_recommendation.py

6. Validate Results
   └─ Run test scripts, review metrics

7. Deploy/Consume
   └─ Use recommendation CSVs in downstream systems
```

---

## 📊 Output Directory Structure

```
s3://bucket/dsp_ga_2025em1100102_20112025/
├── input/raw/
│   ├── company_sales/
│   ├── competitor_sales/
│   └── seller_catalog/
├── output/
│   ├── bronze/
│   │   ├── company_sales/run_date=*/
│   │   ├── competitor_sales/run_date=*/
│   │   └── seller_catalog/run_date=*/
│   ├── silver/
│   │   ├── company_sales/
│   │   ├── competitor_sales/
│   │   └── seller_catalog/
│   └── quarantine/
│       ├── company_sales/run_date=*/
│       ├── competitor_sales/run_date=*/
│       └── seller_catalog/run_date=*/
└── processed/
    ├── gold/
    │   ├── company_sales_hudi/
    │   ├── competitor_sales_hudi/
    │   └── seller_catalog_hudi/
    └── recommendations/
        ├── company/
        │   └── company_seller_recommendation.csv
        └── competitor/
            └── competitor_seller_recommendation.csv
```

---

## 🎓 Learning Path

**Beginner:**
1. Read PROJECT_OVERVIEW.md
2. Follow SETUP_GUIDE.md
3. Run first ETL script
4. Check outputs in S3

**Intermediate:**
1. Review TECHNICAL_GUIDE.md
2. Study ETL scripts
3. Understand DQ rules
4. Run test scripts

**Advanced:**
1. Study CODEBASE_STRUCTURE.md
2. Modify DQ rules
3. Optimize performance
4. Extend functionality

---

## 📝 Version Info

- **Project ID:** 2025em1100102
- **Spark Version:** 3.5.2
- **Hudi Version:** 0.15.0
- **Python:** 3.8+
- **Last Updated:** November 2025

---

## 🔗 External Resources

- [Apache Spark Docs](https://spark.apache.org/docs/latest/)
- [Apache Hudi Docs](https://hudi.apache.org/)
- [AWS S3 Guide](https://docs.aws.amazon.com/s3/)
- [Medallion Architecture](https://www.databricks.com/blog/2022/06/24/use-the-medallion-architecture-to-build-data-pipelines.html)

---

**Quick Links:**
- 📖 Full Documentation: See PROJECT_OVERVIEW.md
- 🔧 Setup Instructions: See SETUP_GUIDE.md
- 💻 Code Reference: See TECHNICAL_GUIDE.md
- 📂 File Organization: See CODEBASE_STRUCTURE.md
