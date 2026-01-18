CREATE OR REPLACE VIEW crypto_lakehouse.vw_quotes_latest AS
SELECT
  coin_id,
  symbol,
  name,
  price_usd,
  market_cap_usd,
  total_volume_usd,
  market_cap_rank,
  event_time_ts AS as_of_ts
FROM crypto_lakehouse.gold_quotes_latest;
