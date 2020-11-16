CREATE VIEW tick_subscription AS
SELECT
  wv.client_id,
  ROW(
    CAST(ts.tick_number AS VARCHAR)
  ) payload
FROM write_value AS wv
JOIN (
  SELECT
    LAST_VALUE(tick_number) tick_number
  FROM services.tick 
) AS ts 
  ON true