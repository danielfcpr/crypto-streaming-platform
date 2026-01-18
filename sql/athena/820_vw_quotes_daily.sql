CREATE OR REPLACE VIEW crypto_lakehouse.vw_quotes_daily AS
SELECT
  dt,
  coin_id,
  symbol,
  name,
  vs_currency,

  avg_price_usd,
  min_price_usd,
  max_price_usd,
  last_price_usd,

  max_market_cap_usd,
  max_total_volume_usd
FROM crypto_lakehouse.gold_quotes_daily;

