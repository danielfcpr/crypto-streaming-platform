CREATE EXTERNAL TABLE IF NOT EXISTS crypto_lakehouse.gold_quotes_daily (
  coin_id                 string,
  symbol                  string,
  name                    string,
  vs_currency             string,
  avg_price_usd           double,
  min_price_usd           double,
  max_price_usd           double,
  last_price_usd          double,
  max_market_cap_usd      double,
  max_total_volume_usd    double
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://s3-crypto-streaming-bucket/gold/daily_symbol/';
