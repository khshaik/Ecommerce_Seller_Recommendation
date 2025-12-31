================================================
Commands

DSP_GA_2025em1100102_20112025
    |- 2025em1100102

cd DSP_GA_2025em1100102_20112025/

DSP_GA_2025em1100102_20112025 % python3 -m venv .venv
DSP_GA_2025em1100102_20112025 % source .venv/bin/activate

(.venv) YUIOP@HQN473V4C2 ctx % pip install pyspark==3.5.2 delta-spark==3.2.0
(.venv) YUIOP@HQN473V4C2 ctx % pip install pyarrow
(.venv) YUIOP@HQN473V4C2 ctx % pip install pyyaml
(.venv) YUIOP@HQN473V4C2 ctx % pip install s3fs
(.venv) YUIOP@HQN473V4C2 ctx % pip install pandas

(.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % (.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  2025em1100102/ecommerce_seller_recommendation/s3/src/etl_company_sales.py \
  --config 2025em1100102/ecommerce_seller_recommendation/s3/configs/ecomm_prod.yml

(.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % (.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  2025em1100102/ecommerce_seller_recommendation/s3/src/etl_seller_catalog.py \
  --config 2025em1100102/ecommerce_seller_recommendation/s3/configs/ecomm_prod.yml

(.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % (.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % (.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  2025em1100102/ecommerce_seller_recommendation/s3/src/etl_competitor_sales.py \
  --config 2025em1100102/ecommerce_seller_recommendation/s3/configs/ecomm_prod.yml 

PYSPARK_SUBMIT_ARGS="--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryo.registrationRequired=false \
--driver-java-options '-Dfs.s3a.connection.timeout=60000 -Dfs.s3a.connection.establish.timeout=5000 -Dfs.s3a.attempts.maximum=5 -Dfs.s3a.connection.maximum=200' \
--driver-class-path /Users/YUIOP/Desktop/jars/woodstox-core-6.2.9.jar:/Users/YUIOP/Desktop/jars/stax2-api-4.2.1.jar \
--jars /Users/YUIOP/Desktop/jars/hadoop-aws-3.3.4.jar,/Users/YUIOP/Desktop/jars/hadoop-common-3.3.4.jar,/Users/YUIOP/Desktop/jars/aws-java-sdk-bundle-1.12.696.jar,/Users/YUIOP/Desktop/jars/delta-spark_2.12-3.2.0.jar,/Users/YUIOP/Desktop/jars/delta-storage-3.2.0.jar,/Users/YUIOP/Desktop/jars/hudi-spark3.5-bundle_2.12-1.0.2.jar pyspark-shell" \
python -m 2025em1100102.ecommerce_seller_recommendation.local.src.consumption_recommendation \
--config 2025em1100102/ecommerce_seller_recommendation/local/config/ecomm_prod.yml --mode both (both or company or competitor)      

(.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  2025em1100102/ecommerce_seller_recommendation/s3/src/consumption_recommendation.py \
  --config 2025em1100102/ecommerce_seller_recommendation/s3/configs/ecomm_prod.yml       

(.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % ./2025em1100102/ecommerce_seller_recommendation/s3/scripts/etl_company_sales_spark_submit.sh
(.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % ./2025em1100102/ecommerce_seller_recommendation/s3/scripts/etl_seller_catalog_spark_submit.sh
(.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % ./2025em1100102/ecommerce_seller_recommendation/s3/scripts/etl_competitor_sales_spark_submit.sh
(.venv) YUIOPQ@HQN473V4C2 DSP_GA_2025em1100102_20112025 % ./2025em1100102/ecommerce_seller_recommendation/s3/scripts/consumption_recommendation_spark_submit.sh

================================================
Project layout

DSP_GA_2025EM1100102_20112025
├── .venv
└── 2025em1100102
    └── ecommerce_seller_recommendation
        ├── s3
        │   ├── conf
        │   │   └── hudi-defaults.conf
        │   ├── configs
        │   │   ├── ecomm_local.yml
        │   │   └── ecomm_prod.yml
        │   └── data
        │       ├── dqcheck
        │       │   ├── company_sales_dirty.csv
        │       │   ├── competitor_sales_dirty.csv
        │       │   └── seller_catalog_dirty.csv
        │       │
        │       └── local
        │           ├── clean
        │           │   └── category / non-category
        │           │       ├── input
        │           │       │   └── raw
        │           │       │       ├── company_sales/company_sales_clean.csv
        │           │       │       ├── competitor_sales/competitor_sales_clean.csv
        │           │       │       └── seller_catalog/seller_catalog_clean.csv
        │           │       ├── output
        │           │       │   ├── bronze
        │           │       │   │   ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │       │   │   ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │       │   │   └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │           │       │   ├── quarantine
        │           │       │   │   ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │       │   │   ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │       │   │   └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │           │       │   └── silver
        │           │       │       ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │       │       ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │       │       └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │           │       └── processed
        │           │           ├── gold
        │           │           │   ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │           │   ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │           │   └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │           │           └── recommendations
        │           │               ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │               ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │               └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │
        │           ├── dirty
        │           │   └── category / non-category
        │           │       ├── input
        │           │       │   └── raw
        │           │       │       ├── company_sales/company_sales_dirty.csv
        │           │       │       ├── competitor_sales/competitor_sales_dirty.csv
        │           │       │       └── seller_catalog/seller_catalog_dirty.csv
        │           │       ├── output
        │           │       │   ├── bronze
        │           │       │   │   ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │       │   │   ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │       │   │   └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │           │       │   ├── quarantine
        │           │       │   │   ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │       │   │   ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │       │   │   └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │           │       │   └── silver
        │           │       │       ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │       │       ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │       │       └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │           │       └── processed
        │           │           ├── gold
        │           │           │   ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │           │   ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │           │   └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │           │           └── recommendations
        │           │               ├── company_sales/run_date=2025-11-21_195457/*.csv
        │           │               ├── competitor_sales/run_date=2025-11-21_195457/*.csv
        │           │               └── seller_catalog/run_date=2025-11-21_195457/*.csv
        │
        ├── helper
        │   ├── dqcheck
        │   └── logs
        │       ├── clean
        │       │   ├── etl/logs.txt
        │       │   └── recommendations/logs.txt
        │       └── dirty
        │           ├── etl/logs.txt
        │           └── recommendations/logs.txt
        │
        ├── commands.txt
        ├── output_structure.txt
        ├── project_structure.txt
        │
        ├── libs
        │   ├── dq.py
        │   ├── log.py
        │   ├── spark_session.py
        │   └── utils.py
        │
        ├── scripts
        │   ├── analysis
        │   │   ├── category/consumption_recommendation_spark_submit.sh
        │   │   └── noncategory/consumption_recommendation_spark_submit.sh
        │   ├── spark-warehouse
        │   ├── consumption_recommendation_spark_submit.sh
        │   ├── etl_company_sales_spark_submit.sh
        │   ├── etl_competitor_sales_spark_submit.sh
        │   └── etl_seller_catalog_spark_submit.sh
        │
        ├── src
        │   ├── __pycache__
        │   ├── analysis
        │   ├── consumption_recommendation.py
        │   └── etl_company_sales.py
        │
        └── tests
            ├── analyze_already_owned.py
            ├── test_consumption_recommendations.py
            ├── validate_expected_units.py
            └── validate_logic.py

================================================

Output files structure/layout in S3 bucket

2025em1100102
└── dsp_ga_2025em1100102_20112025
    ├── clean
    │   └── category/ non-category
    |        └── input
    │               └── raw
    │                    ├── company_sales - company_sales_clean.csv
    │                    ├── competitor_sales - competitor_sales_clean.csv
    │                    └── seller_catalog - seller_catalog_clean.csv
    |        └── output
    │               └── bronze
    │                    ├── company_sales - run_date=2025-11-21_195457/*.csv
    │                    ├── competitor_sales - run_date=2025-11-21_195457/*.csv
    │                    └── seller_catalog - run_date=2025-11-21_195457/*.csv
    │               └── quarantine
    │                    ├── company_sales - run_date=2025-11-21_195457/*.csv
    │                    ├── competitor_sales - run_date=2025-11-21_195457/*.csv
    │                    └── seller_catalog - run_date=2025-11-21_195457/*.csv   
    │               └── silver
    │                    ├── company_sales - run_date=2025-11-21_195457/*.csv
    │                    ├── competitor_sales - run_date=2025-11-21_195457/*.csv
    │                    └── seller_catalog - run_date=2025-11-21_195457/*.csv
    |        └── processed
    │               └── gold
    │                    ├── company_sales - run_date=2025-11-21_195457/*.csv
    │                    ├── competitor_sales - run_date=2025-11-21_195457/*.csv
    │                    └── seller_catalog - run_date=2025-11-21_195457/*.csv
    │               └── recommendations
    │                    ├── company_sales - run_date=2025-11-21_195457/*.csv
    │                    ├── competitor_sales - run_date=2025-11-21_195457/*.csv
    │                    └── seller_catalog - run_date=2025-11-21_195457/*.csv   
    ├── dirty
    │   └── category/ non-category
    |        └── input
    │               └── raw
    │                    ├── company_sales - company_sales_clean.csv
    │                    ├── competitor_sales - competitor_sales_clean.csv
    │                    └── seller_catalog - seller_catalog_clean.csv
    |        └── output
    │               └── bronze
    │                    ├── company_sales - run_date=2025-11-21_195457/*.csv
    │                    ├── competitor_sales - run_date=2025-11-21_195457/*.csv
    │                    └── seller_catalog - run_date=2025-11-21_195457/*.csv
    │               └── quarantine
    │                    ├── company_sales - run_date=2025-11-21_195457/*.csv
    │                    ├── competitor_sales - run_date=2025-11-21_195457/*.csv
    │                    └── seller_catalog - run_date=2025-11-21_195457/*.csv   
    │               └── silver
    │                    ├── company_sales - run_date=2025-11-21_195457/*.csv
    │                    ├── competitor_sales - run_date=2025-11-21_195457/*.csv
    │                    └── seller_catalog - run_date=2025-11-21_195457/*.csv
    |        └── processed
    │               └── gold
    │                    ├── company_sales - run_date=2025-11-21_195457/*.csv
    │                    ├── competitor_sales - run_date=2025-11-21_195457/*.csv
    │                    └── seller_catalog - run_date=2025-11-21_195457/*.csv
    │               └── recommendations
    │                    ├── company - run_date=2025-11-21_195457/company_seller_recommendation.csv
    │                    ├── competitor - run_date=2025-11-21_195457/competitor_seller_recommendation.csv