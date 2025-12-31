import os
import yaml
from pyspark.sql import SparkSession

def get_spark(app_name="ETL"):
    print("🔧 Initializing SparkSession with full Delta + S3 + XML support...")

    jars_path = "/Users/81194246/Desktop/jars"
    jar_files = [
        f"{jars_path}/hadoop-aws-3.3.6.jar",
        f"{jars_path}/hadoop-common-3.3.6.jar",
        f"{jars_path}/aws-java-sdk-bundle-1.12.696.jar",
        f"{jars_path}/woodstox-core-6.2.9.jar",
        f"{jars_path}/stax2-api-4.2.1.jar"
        f"{jars_path}/hudi-spark3.5-bundle_2.12-1.0.2.jar"
    ]
    jars_classpath = ",".join(jar_files)

    builder = (
        SparkSession.builder
        .appName(app_name)
        # Use both Delta and S3 jars
        #.config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
        .config("spark.jars", jars_classpath)
        .config("spark.driver.extraClassPath", ":".join(jar_files))
        .config("spark.executor.extraClassPath", ":".join(jar_files))

        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Delta extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # S3 configs
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "5")
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .config(" spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        .config("spark.hadoop.fs.s3a.retry.interval", "2000")
        .config("spark.hadoop.fs.s3a.retry.throttle.interval", "2000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
        .config("spark.hadoop.fs.s3a.multipart.uploads.age", "86400000")

        # Tuning
        .config("spark.sql.shuffle.partitions", "2")
        
        # Add Hudi-required serializer configs
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .config("spark.kryo.registrator", "org.apache.hudi.HoodieSparkKryoRegistrar")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "ecomm_prod.yml")
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    print("✅ SparkSession created successfully!")
    print(f"🪪 App Name     : {spark.sparkContext.appName}")
    print(f"⚙️  Spark Version : {spark.version}")
    print(f"🧠 Master       : {spark.sparkContext.master}")

    
    return spark, cfg


if __name__ == "__main__":
    print("🚀 Testing SparkSession initialization...")
    spark, cfg = get_spark("SparkSessionTest")

    try:
        print("🔍 Testing S3 access...")
        df = spark.read.text("s3a://bits-2025em1100102-s3-global-access/lab5/input/customers/")
        df.show(3)
    except Exception as e:
        print("❌ S3 test failed:", e)

    spark.stop()