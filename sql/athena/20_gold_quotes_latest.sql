CREATE EXTERNAL TABLE IF NOT EXISTS crypto_lakehouse.gold_quotes_latest (
  coin_id                 string,
  symbol                  string,
  name                    string,
  vs_currency             string,
  price_usd               double,
  market_cap_rank         int,
  market_cap_usd          double,
  total_volume_usd        double,
  last_updated_ts         timestamp,
  event_time_ts           timestamp
)

STORED AS PARQUET
LOCATION 's3://s3-crypto-streaming-bucket/gold/latest_snapshot/';