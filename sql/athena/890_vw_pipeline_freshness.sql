CREATE OR REPLACE VIEW crypto_lakehouse.vw_latest_freshness AS
SELECT
  'gold_quotes_latest' AS dataset,
  max(event_time_ts)   AS last_event_ts,
  CAST(current_timestamp AS timestamp) AS computed_at_ts,
  date_diff(
    'minute',
    max(event_time_ts),
    CAST(current_timestamp AS timestamp)
  ) AS freshness_minutes
FROM crypto_lakehouse.gold_quotes_latest;