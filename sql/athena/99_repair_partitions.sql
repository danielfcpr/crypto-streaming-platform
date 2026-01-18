-- Refresh partitions after Spark writes new data
MSCK REPAIR TABLE crypto_lakehouse.gold_quotes_daily;
MSCK REPAIR TABLE crypto_lakehouse.gold_quotes_1m;
