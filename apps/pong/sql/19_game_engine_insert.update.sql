CREATE VIEW game_in_v AS
SELECT
  CAST(ts.tick_number            AS VARCHAR) AS tick_number           , -- si CAST INTEGER: Unsupported cast from 'ROW' to 'ROW'.
  CAST(ts.proctime_append_stream AS VARCHAR) AS proctime_append_stream,
  CAST(leftY                     AS VARCHAR) AS leftY                 , -- lié au non détail de Row FLINK-15584
  CAST(rightY                    AS VARCHAR) AS rightY                  -- lié au non détail de Row FLINK-15584
FROM (
  SELECT *
  FROM services.tick
) AS ts
   , LATERAL TABLE (global_position_temporal(ts.proctime_append_stream)) AS gpt
WHERE ts.constant_dummy_source = gpt.dummy_key
