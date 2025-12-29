import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType
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


def main():
    bucket = os.environ.get("S3_BUCKET_NAME")
    if not bucket:
        raise ValueError("Missing env var S3_BUCKET_NAME")

    bronze_prefix = os.environ.get("BRONZE_PREFIX", "bronze/crypto_quotes_raw")
    silver_history_prefix = os.environ.get("SILVER_HISTORY_PREFIX", "silver/crypto_quotes_history")
    silver_latest_prefix = os.environ.get("SILVER_LATEST_PREFIX", "silver/crypto_quotes_latest")

    bronze_path = f"s3a://{bucket}/{bronze_prefix}/"
    out_history = f"s3a://{bucket}/{silver_history_prefix}/"
    out_latest = f"s3a://{bucket}/{silver_latest_prefix}/"

    spark = build_spark("crypto-bronze-to-silver")

    schema = StructType([
        StructField("price_usd", DoubleType(), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("symbol", StringType(), True),
        StructField("last_updated", StringType(), True),
        StructField("coin_id", StringType(), True),
        StructField("market_cap_rank", IntegerType(), True),
        StructField("vs_currency", StringType(), True),
        StructField("ingestion_run_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("market_cap_usd", LongType(), True),
        StructField("total_volume_usd", LongType(), True),
        StructField("name", StringType(), True),
        StructField("event_time", StringType(), True),
    ])

    # Reads newline-delimited JSON (your S3 files have 1 JSON per line)
    df_raw = (
        spark.read
        .schema(schema)
        .json(bronze_path)
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_time", F.current_timestamp())
    )

    # Parse timestamps safely
    df = (
        df_raw
        .withColumn("event_time_ts", F.to_timestamp("event_time"))
        .withColumn("last_updated_ts", F.to_timestamp("last_updated"))
        .withColumn("dt", F.to_date("event_time_ts"))
        .filter(F.col("coin_id").isNotNull())
        .filter(F.col("event_time_ts").isNotNull())
    )

    # 1) Silver HISTORY (append)
    (
        df.select(
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
        )
        .write
        .mode("append")
        .partitionBy("dt")
        .parquet(out_history)
    )

    # 2) Silver LATEST (overwrite only dt partitions we touched)
    # Keep latest per (dt, coin_id) by event_time_ts
    w = Window.partitionBy("dt", "coin_id").orderBy(F.col("event_time_ts").desc(), F.col("last_updated_ts").desc_nulls_last())
    df_latest = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    (
        df_latest.select(
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
        )
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(out_latest)
    )

    print("✅ Silver history written to:", out_history)
    print("✅ Silver latest written to:", out_latest)

    spark.stop()


if __name__ == "__main__":
    main()
