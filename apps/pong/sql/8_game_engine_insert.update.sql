INSERT INTO game_engine_in
SELECT
  ROW(
    CAST(ts.tick_number            AS VARCHAR), -- si CAST INTEGER: Unsupported cast from 'ROW' to 'ROW'.
    CAST(ts.proctime_append_stream AS VARCHAR),
    CAST(leftY AS VARCHAR), -- lié au non détail de Row FLINK-15584
    CAST(rightY AS VARCHAR) -- lié au non détail de Row FLINK-15584
  ) AS payload
FROM (
  SELECT *
  FROM services.tick
) AS ts
   , LATERAL TABLE (global_position_temporal(ts.proctime_append_stream)) AS gpt
WHERE ts.constant_dummy_source = gpt.dummy_key
