from typing import Dict, List
from pyspark.sql import DataFrame, functions as F, Window

def enforce_not_null(df: DataFrame, cols: List[str]) -> DataFrame:
    for c in cols:
        df = df.where(F.col(c).isNotNull())
    return df

def enforce_positive(df: DataFrame, rules: Dict[str, float]) -> DataFrame:
    for c, min_val in rules.items():
        df = df.where(F.col(c) > F.lit(min_val))
    return df

def enforce_regex(df: DataFrame, col: str, pattern: str) -> DataFrame:
    return df.where(F.col(col).rlike(pattern))

def enforce_allowed_values(df: DataFrame, col: str, allowed: List[str]) -> DataFrame:
    return df.where(F.col(col).isin(allowed))

def enforce_max(df: DataFrame, rules: Dict[str, float]) -> DataFrame:
    for c, mx in rules.items():
        df = df.where(F.col(c) <= F.lit(mx))
    return df

def dedupe_by_latest(df: DataFrame, key_cols: List[str], ts_col: str) -> DataFrame:
    w = Window.partitionBy(*key_cols).orderBy(F.col(ts_col).desc())
    return (df
            .withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn")==1)
            .drop("_rn"))

