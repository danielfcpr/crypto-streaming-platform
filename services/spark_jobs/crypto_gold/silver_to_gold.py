import os
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType, TimestampType, DateType, BooleanType
)

def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    # S3A auth
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    )
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

    return spark

# Contracts
REQUIRED_COLS = [
    "dt", "coin_id", "symbol", "name",
    "event_time_ts",
    "price_usd", "market_cap_usd", "total_volume_usd",
    "vs_currency"
]

def enforce_contract(df):
    """
    Normalize schema + types for Gold.
    """
    out = (
        df
        .withColumn("dt", F.col("dt").cast(DateType()))
        .withColumn("event_time_ts", F.col("event_time_ts").cast(TimestampType()))
        .withColumn("last_updated_ts", F.col("last_updated_ts").cast(TimestampType()))
        .withColumn("coin_id", F.col("coin_id").cast(StringType()))
        .withColumn("symbol", F.col("symbol").cast(StringType()))
        .withColumn("name", F.col("name").cast(StringType()))
        .withColumn("vs_currency", F.col("vs_currency").cast(StringType()))
        .withColumn("market_cap_rank", F.col("market_cap_rank").cast(IntegerType()))
        .withColumn("price_usd", F.col("price_usd").cast(DoubleType()))
        .withColumn("price_change_percentage_24h", F.col("price_change_percentage_24h").cast(DoubleType()))
        .withColumn("market_cap_usd", F.col("market_cap_usd").cast(DoubleType()))
        .withColumn("total_volume_usd", F.col("total_volume_usd").cast(DoubleType()))
    )

    # Select explicit columns to avoid accidental drift
    cols = [
        "dt",
        "event_time_ts",
        "last_updated_ts",
        "ingestion_time",
        "ingestion_run_id",
        "source",
        "vs_currency",
        "coin_id",
        "symbol",
        "name",
        "market_cap_rank",
        "price_usd",
        "price_change_percentage_24h",
        "market_cap_usd",
        "total_volume_usd",
        "source_file",
    ]
    existing = [c for c in cols if c in out.columns]
    return out.select(*existing)


def dq_check_required_cols(df):
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        return True, [f"missing_columns:{missing}"]
    return False, []


def dq_check_nulls(df):
    """
    Basic null checks on required columns.
    """
    reasons = []
    for c in REQUIRED_COLS:
        bad = df.filter(F.col(c).isNull()).limit(1).count()
        if bad > 0:
            reasons.append(f"null_{c}")
    return (len(reasons) > 0), reasons


def dq_check_price_positive(df):
    reasons = []
    bad = df.filter(F.col("price_usd").isNull() | (F.col("price_usd") <= 0)).limit(1).count()
    if bad > 0:
        reasons.append("price_usd_null_or_non_positive")
    return (len(reasons) > 0), reasons


def freshness_check(df, max_lag_minutes: int):
    """
    Ensure data isn't stale based on max(event_time_ts).
    """
    mx = df.select(F.max("event_time_ts").alias("mx")).collect()[0]["mx"]
    if mx is None:
        return True, ["no_event_time_ts_found"]

    now = datetime.now(timezone.utc)
    lag_min = (now - mx.replace(tzinfo=timezone.utc)).total_seconds() / 60.0
    if lag_min > max_lag_minutes:
        return True, [f"freshness_lag_minutes={lag_min:.1f}"]
    return False, []


def deduplicate_quotes(df):
    """
    For Gold quotes, dedup on (dt, coin_id, event_time_ts) keeping latest last_updated_ts.
    This is a reasonable “uniqueness” enforcement without requiring a hard constraint system.
    """
    w = (
        Window.partitionBy("dt", "coin_id", "event_time_ts")
        .orderBy(F.col("last_updated_ts").desc_nulls_last(), F.col("ingestion_time").desc_nulls_last())
    )
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")



# Main job
def main():
    bucket = os.environ.get("S3_BUCKET_NAME")
    if not bucket:
        raise ValueError("Missing env var S3_BUCKET_NAME")

    # Silver inputs
    silver_history_prefix = os.environ.get("SILVER_HISTORY_PREFIX", "silver/crypto_quotes_history")
    silver_latest_prefix = os.environ.get("SILVER_LATEST_PREFIX", "silver/crypto_quotes_latest")

    # Gold outputs
    gold_quotes_prefix = os.environ.get("GOLD_QUOTES_PREFIX", "gold/quotes_1m")
    gold_daily_prefix = os.environ.get("GOLD_DAILY_PREFIX", "gold/daily_symbol")
    gold_snapshot_prefix = os.environ.get("GOLD_SNAPSHOT_PREFIX", "gold/latest_snapshot")

    # Quality settings
    max_lag_minutes = int(os.environ.get("GOLD_FRESHNESS_MAX_LAG_MINUTES", "20000"))
    shuffle_partitions = int(os.environ.get("GOLD_SHUFFLE_PARTITIONS", "32"))

    in_history = f"s3a://{bucket}/{silver_history_prefix}/"
    in_latest  = f"s3a://{bucket}/{silver_latest_prefix}/"

    out_quotes = f"s3a://{bucket}/{gold_quotes_prefix}/"
    out_daily = f"s3a://{bucket}/{gold_daily_prefix}/"
    out_snapshot = f"s3a://{bucket}/{gold_snapshot_prefix}/"

    spark = build_spark("crypto-silver-to-gold")
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))

    # Read from Silver history (append-only base for analytics)
    df_hist = spark.read.parquet(in_history)
    df_hist = enforce_contract(df_hist)
    df_hist = deduplicate_quotes(df_hist)

    # --- DQ gates
    failed, reasons = dq_check_required_cols(df_hist)
    if failed:
        raise RuntimeError(f"DQ failed: {reasons}")

    failed2, reasons2 = dq_check_nulls(df_hist)
    failed3, reasons3 = dq_check_price_positive(df_hist)
    failed4, reasons4 = freshness_check(df_hist, max_lag_minutes=max_lag_minutes)

    dq_failed = failed2 or failed3 or failed4
    dq_reasons = reasons2 + reasons3 + reasons4

    if dq_failed:
        raise RuntimeError(f"DQ/Freshness failed: {dq_reasons}")

    # Gold A: quotes_1m (fact)
    df_quotes = df_hist.select(
        "dt",
        "event_time_ts",
        "last_updated_ts",
        "coin_id",
        "symbol",
        "name",
        "vs_currency",
        "market_cap_rank",
        "price_usd",
        "price_change_percentage_24h",
        "market_cap_usd",
        "total_volume_usd",
        "source",
    )

    # Write compact-ish by partition
    (
        df_quotes
        .repartition("dt")
        .write
        .mode("append")
        .partitionBy("dt")
        .parquet(out_quotes)
    )

    # Gold B: daily_symbol_metrics (agg)
    df_daily = (
        df_quotes.groupBy("dt", "coin_id", "symbol", "name", "vs_currency")
        .agg(
            F.avg("price_usd").alias("avg_price_usd"),
            F.min("price_usd").alias("min_price_usd"),
            F.max("price_usd").alias("max_price_usd"),
            F.max(F.struct("event_time_ts", "price_usd")).alias("_last"),
            F.max("market_cap_usd").alias("max_market_cap_usd"),
            F.max("total_volume_usd").alias("max_total_volume_usd"),
        )
        .withColumn("last_price_usd", F.col("_last")["price_usd"])
        .drop("_last")
    )

    (
        df_daily
        .repartition("dt")
        .write
        .mode("append")
        .partitionBy("dt")
        .parquet(out_daily)
    )

    # Gold C: latest_snapshot (overwrite)
    # Read from Silver latest, contract, keep latest per coin_id overall
    df_latest = spark.read.parquet(in_latest)
    df_latest = enforce_contract(df_latest)

    # Keep one row per coin_id (the most recent event_time_ts)
    w = Window.partitionBy("coin_id").orderBy(F.col("event_time_ts").desc(), F.col("last_updated_ts").desc_nulls_last())
    df_snapshot = df_latest.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    (
df_snapshot
        .drop("dt")
        .write
        .mode("overwrite")
        .parquet(out_snapshot)
    )

    print("✅ Gold quotes written to:", out_quotes)
    print("✅ Gold daily metrics written to:", out_daily)
    print("✅ Gold snapshot written to:", out_snapshot)

    spark.stop()


if __name__ == "__main__":
    main()

