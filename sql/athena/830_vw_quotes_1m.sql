CREATE OR REPLACE VIEW crypto_lakehouse.vw_quotes_1m AS
SELECT
  dt,
  event_time_ts,
  last_updated_ts,
  coin_id,
  upper(symbol) AS symbol,
  name,
  vs_currency,
  market_cap_rank,
  price_usd,
  price_change_percentage_24h,
  market_cap_usd,
  total_volume_usd,
  source
FROM crypto_lakehouse.gold_quotes_1m;
