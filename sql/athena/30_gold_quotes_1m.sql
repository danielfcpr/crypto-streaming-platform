CREATE EXTERNAL TABLE IF NOT EXISTS crypto_lakehouse.gold_quotes_1m (
  event_time_ts               timestamp,
  last_updated_ts             timestamp,
  coin_id                     string,
  symbol                      string,
  name                        string,
  vs_currency                 string,
  market_cap_rank             int,
  price_usd                   double,
  price_change_percentage_24h double,
  market_cap_usd              double,
  total_volume_usd            double,
  source                      string
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://crypto-streaming-bronze/gold/quotes_1m/';
