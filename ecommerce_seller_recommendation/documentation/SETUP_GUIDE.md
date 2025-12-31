# Setup & Installation Guide

## 🖥️ System Requirements

### Minimum Requirements
- **OS:** macOS, Linux, or Windows (with WSL2)
- **Python:** 3.8 or higher
- **Java:** JDK 11 or higher
- **RAM:** 8GB minimum (16GB recommended)
- **Disk Space:** 20GB for Spark, dependencies, and data

### Recommended Setup
- **OS:** macOS 12+ or Ubuntu 20.04+
- **Python:** 3.10+
- **Java:** JDK 17
- **RAM:** 32GB+
- **Disk Space:** 50GB+

---

## 📦 Installation Steps

### Step 1: Verify Prerequisites

```bash
# Check Python version
python3 --version
# Expected: Python 3.8+

# Check Java installation
java -version
# Expected: Java 11+

# Check if Spark is available
spark-submit --version
# If not installed, follow Spark installation below
```

### Step 2: Install Apache Spark (if not already installed)

#### **macOS (using Homebrew)**
```bash
brew install apache-spark
```

#### **Linux (Ubuntu/Debian)**
```bash
# Download Spark
wget https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz

# Extract
tar -xzf spark-3.5.2-bin-hadoop3.tgz
sudo mv spark-3.5.2-bin-hadoop3 /opt/spark

# Set environment variables
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

#### **macOS (Manual)**
```bash
# Download Spark
wget https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz

# Extract
tar -xzf spark-3.5.2-bin-hadoop3.tgz
sudo mv spark-3.5.2-bin-hadoop3 /opt/spark

# Set environment variables
echo 'export SPARK_HOME=/opt/spark' >> ~/.zshrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.zshrc
source ~/.zshrc
```

### Step 3: Clone/Access Project

```bash
# Navigate to project directory
cd /Users/81194246/Desktop/Workspace/DS/DataScience/Digital/Year1/Trim1/DataStores&Pipelines/graded_assignment1/2025em1100102/ecommerce_seller_recommendation/final/DSP_GA_2025em1100102_20112025/

# Verify structure
ls -la
# Should show: 2025em1100102 directory
```

### Step 4: Create Python Virtual Environment

```bash
# Create venv
python3 -m venv .venv

# Activate venv
source .venv/bin/activate
# On Windows: .venv\Scripts\activate

# Verify activation
which python
# Should show: /path/to/.venv/bin/python
```

### Step 5: Install Python Dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Install core dependencies
pip install pyspark==3.5.2
pip install delta-spark==3.2.0
pip install pyarrow==14.0.0
pip install pyyaml==6.0
pip install s3fs==2023.12.0
pip install pandas==2.1.0

# Verify installations
python -c "import pyspark; print(pyspark.__version__)"
# Expected: 3.5.2
```

### Step 6: Download Required JAR Files

The project requires specific JAR files for Hudi, Hadoop, and AWS support.

```bash
# Create jars directory
mkdir -p ~/Desktop/jars
cd ~/Desktop/jars

# Download required JARs
# Hadoop AWS
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Hadoop Common
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar

# AWS Java SDK Bundle
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.696/aws-java-sdk-bundle-1.12.696.jar

# Woodstox (XML processing)
wget https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.2.9/woodstox-core-6.2.9.jar

# Stax2 API
wget https://repo1.maven.org/maven2/org/codehaus/stax2-api/4.2.1/stax2-api-4.2.1.jar

# Hudi Spark Bundle
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar

# Verify downloads
ls -lh *.jar
```

### Step 7: Configure AWS Credentials

```bash
# Option 1: Using AWS CLI (Recommended)
aws configure
# Enter: AWS Access Key ID
# Enter: AWS Secret Access Key
# Enter: Default region (e.g., ap-south-1)
# Enter: Default output format (json)

# Option 2: Using Environment Variables
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export AWS_DEFAULT_REGION=ap-south-1

# Option 3: Using IAM Role (if running on EC2/ECS)
# No configuration needed - uses instance metadata

# Verify AWS access
aws s3 ls
# Should list your S3 buckets
```

### Step 8: Update Project Configuration

Edit the configuration file:

```bash
# Open config file
nano 2025em1100102/ecommerce_seller_recommendation/s3/configs/ecomm_prod.yml
```

**Key configurations to update:**

```yaml
# Update S3 bucket name if different
paths:
  input_root: "s3a://YOUR-BUCKET-NAME/dsp_ga_2025em1100102_20112025/input/raw"
  output_root: "s3a://YOUR-BUCKET-NAME/dsp_ga_2025em1100102_20112025/output"
  gold_root: "s3a://YOUR-BUCKET-NAME/dsp_ga_2025em1100102_20112025/processed/"

# Update JAR paths if different
spark:
  jars_path: "/Users/YOUR-USERNAME/Desktop/jars"
```

### Step 9: Verify Installation

```bash
# Test Spark
spark-submit --version

# Test Python imports
python << 'EOF'
import pyspark
import delta
import yaml
import pandas
print("✅ All imports successful!")
print(f"PySpark version: {pyspark.__version__}")
EOF

# Test S3 access
python << 'EOF'
import boto3
s3 = boto3.client('s3')
response = s3.list_buckets()
print("✅ S3 access successful!")
print(f"Buckets: {[b['Name'] for b in response['Buckets']]}")
EOF
```

---

## 🔧 Configuration Details

### Spark Configuration (`ecomm_prod.yml`)

```yaml
spark:
  shuffle_partitions: 8           # Adjust based on cluster size
  log_level: "WARN"               # WARN, INFO, DEBUG
  configs:
    spark.sql.adaptive.enabled: "true"              # Adaptive query execution
    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
    spark.hadoop.fs.s3a.path.style.access: "true"  # Virtual-hosted-style URLs
    spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version: "2"
    spark.hadoop.fs.s3a.connection.timeout: "60000"  # 60 seconds
    spark.hadoop.fs.s3a.attempts.maximum: "5"        # Retry attempts
```

### Data Quality Rules

```yaml
dq:
  # Required columns (must not be null)
  company_sales_required:
    - item_id
    - seller_id
    - units_sold
    - revenue
    - sale_date

  # Numeric validation
  company_sales_numeric_checks:
    units_sold_min: 0
    revenue_min: 0
    marketplace_price_min: 0

  # Date format support
  company_sales_date_checks:
    max_date_today: true
    allowed_formats:
      - "dd/MM/yyyy"
      - "dd/MM/yy"
      - "yyyy-MM-dd"
```

---

## 🚀 Running Your First ETL Job

### Quick Test Run

```bash
# Activate virtual environment
source .venv/bin/activate

# Navigate to project root
cd DSP_GA_2025em1100102_20112025/

# Run company sales ETL
spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  2025em1100102/ecommerce_seller_recommendation/s3/src/etl_company_sales.py \
  --config 2025em1100102/ecommerce_seller_recommendation/s3/configs/ecomm_prod.yml
```

### Expected Output

```
✅ SparkSession created successfully!
🪪 App Name     : etl_company_sales
⚙️  Spark Version : 3.5.2
🧠 Master       : local[*]

📊 Reading raw data from: s3a://bucket/path/company_sales_dirty.csv
🔍 Applying DQ rules...
✅ Valid records: 903,696
⚠️  Quarantine records: 96,304

💾 Writing to Bronze: s3a://bucket/path/output/bronze/company_sales
💾 Writing to Silver: s3a://bucket/path/output/silver/company_sales
💾 Writing Quarantine: s3a://bucket/path/output/quarantine/company_sales

✅ ETL completed successfully!
```

---

## 📊 Local Development Setup

### Using Local File System Instead of S3

For development without S3 access:

```bash
# Create local data directories
mkdir -p data/input/raw/{company_sales,competitor_sales,seller_catalog}
mkdir -p data/output/{bronze,silver,quarantine,gold}

# Copy sample data
cp 2025em1100102/ecommerce_seller_recommendation/s3/data/dqcheck/*.csv \
   data/input/raw/

# Update config for local paths
nano 2025em1100102/ecommerce_seller_recommendation/s3/configs/ecomm_local.yml
```

**Local config example:**

```yaml
paths:
  input_root: "file:///path/to/data/input/raw"
  output_root: "file:///path/to/data/output"
  bronze_company_sales_data: "file:///path/to/data/output/bronze/company_sales"
  silver_company_sales_data: "file:///path/to/data/output/silver/company_sales"
```

---

## 🐛 Troubleshooting

### Issue: "JAVA_HOME not set"

```bash
# Find Java installation
/usr/libexec/java_home

# Set JAVA_HOME
export JAVA_HOME=$(/usr/libexec/java_home)
echo 'export JAVA_HOME=$(/usr/libexec/java_home)' >> ~/.zshrc
```

### Issue: "S3 connection timeout"

```bash
# Increase timeout in config
spark:
  configs:
    spark.hadoop.fs.s3a.connection.timeout: "120000"  # 2 minutes
    spark.hadoop.fs.s3a.attempts.maximum: "10"        # More retries
```

### Issue: "Out of memory"

```bash
# Increase Spark memory
spark-submit \
  --driver-memory 4g \
  --executor-memory 4g \
  --executor-cores 4 \
  ...
```

### Issue: "Hudi table not found"

```bash
# Verify S3 paths
aws s3 ls s3://your-bucket/dsp_ga_2025em1100102_20112025/

# Check Hudi metadata
aws s3 ls s3://your-bucket/path/to/table/.hoodie/
```

### Issue: "DQ failures - too many quarantine records"

```bash
# Review quarantine files
aws s3 cp s3://bucket/output/quarantine/company_sales/ . --recursive

# Analyze failures
head -100 quarantine_records.csv
```

---

## ✅ Verification Checklist

After setup, verify each component:

- [ ] Python 3.8+ installed
- [ ] Java 11+ installed
- [ ] Spark 3.5.2 installed
- [ ] Virtual environment created and activated
- [ ] All Python packages installed
- [ ] JAR files downloaded to `~/Desktop/jars/`
- [ ] AWS credentials configured
- [ ] S3 bucket accessible
- [ ] Configuration files updated with correct paths
- [ ] Test ETL job runs successfully
- [ ] Output files created in S3 or local filesystem

---

## 📚 Next Steps

1. **Run ETL Pipeline:** Follow the Quick Start in PROJECT_OVERVIEW.md
2. **Review Outputs:** Check Bronze, Silver, and Gold layers
3. **Run Recommendations:** Generate seller recommendations
4. **Validate Results:** Use test scripts in `s3/tests/`
5. **Monitor Logs:** Check `helper/logs/` for execution details

---

## 🔗 Useful Commands

```bash
# List S3 contents
aws s3 ls s3://bucket-name/ --recursive

# Copy from S3 to local
aws s3 cp s3://bucket/path/file.csv ./local-file.csv

# Copy from local to S3
aws s3 cp ./local-file.csv s3://bucket/path/file.csv

# Monitor Spark job
# Open http://localhost:4040 while job is running

# Check Spark logs
tail -f /path/to/spark/logs/spark-*.log

# Kill Spark job
pkill -f spark-submit
```

---

**Last Updated:** November 2025  
**Version:** 1.0
