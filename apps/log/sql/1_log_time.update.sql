INSERT INTO services.debug
SELECT
  CAST(tick_number AS VARCHAR)
FROM services.tick
