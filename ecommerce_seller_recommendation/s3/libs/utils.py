from typing import Optional
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable

# ---------- Delta-backed watermark store ----------

def ensure_state_table(spark, path: str):
    if not DeltaTable.isDeltaTable(spark, path):
        empty = spark.createDataFrame([], "dataset STRING, last_ts TIMESTAMP")
        empty.write.format("delta").mode("overwrite").save(path)

def get_last_ts(spark, state_path: str, dataset: str) -> Optional[str]:
    ensure_state_table(spark, state_path)
    df = spark.read.format("delta").load(state_path).where(F.col("dataset")==F.lit(dataset))
    row = df.select(F.col("last_ts").cast("timestamp")).orderBy(F.desc("last_ts")).limit(1).collect()
    return row[0]["last_ts"].isoformat() if row else None

def set_last_ts(spark, state_path: str, dataset: str, last_ts_col: str, df: DataFrame):
    ensure_state_table(spark, state_path)
    max_ts = df.select(F.max(F.col(last_ts_col)).alias("m")).collect()[0]["m"]
    if max_ts is None:
        return
    state = DeltaTable.forPath(spark, state_path)
    staging = spark.createDataFrame([(dataset, max_ts)], "dataset STRING, last_ts TIMESTAMP")
    (state.alias("t")
        .merge(staging.alias("s"), "t.dataset = s.dataset")
        .whenMatchedUpdate(set={"last_ts": "s.last_ts"})
        .whenNotMatchedInsert(values={"dataset":"s.dataset", "last_ts":"s.last_ts"})
        .execute())

# ---------- Generic helpers ----------

def bronze_write(df: DataFrame, path: str):
    df.write.mode("append").format("delta").save(path)

def overwrite_delta(df: DataFrame, path: str, partitionBy=None):
    w = df.write.format("delta").mode("overwrite")
    if partitionBy:
        w = w.partitionBy(partitionBy)
    w.save(path)

