from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadParquetExample")
    .getOrCreate()
)

# Replace with your actual file path
df = spark.read.parquet("/Users/81194246/Desktop/Workspace/CTX/DS/DSP/ecom_etl/data/s3/read/customers/part-00000-e8dad6c4-b84c-4834-a512-ab9d4ca375ee.c000.snappy.parquet")
df.show(10, truncate=False)
df.printSchema()

df = spark.read.parquet("/Users/81194246/Desktop/Workspace/CTX/DS/DSP/ecom_etl/data/s3/read/orders/part-00000-33171dfb-e2cf-4cda-af3a-daa8848f3e59.c000.snappy.parquet")
df.show(10, truncate=False)
df.printSchema()

df = spark.read.parquet("/Users/81194246/Desktop/Workspace/CTX/DS/DSP/ecom_etl/data/s3/read/products/part-00000-8fcf3994-2066-4c33-95f0-aadeac8f14e5.c000.snappy.parquet")
df.show(10, truncate=False)
df.printSchema()

spark.stop()