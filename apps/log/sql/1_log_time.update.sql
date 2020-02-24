INSERT INTO debug_sink
SELECT
  CAST(tick_number AS VARCHAR)
FROM tick_service
