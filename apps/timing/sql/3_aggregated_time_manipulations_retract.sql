CREATE RETRACT VIEW aggregated_time_manipulations_retract AS
SELECT COALESCE(CAST('aggregated time manipulations: '
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  tick_count 
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  max_proctime_append_stream 
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  max_proctime_append_stream - INTERVAL '0.5' SECOND
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''
  AS VARCHAR), '') AS debug_message
FROM (
  SELECT 
    count(*) AS tick_count,
    max(proctime_append_stream) AS max_proctime_append_stream
  FROM services.tick
)
