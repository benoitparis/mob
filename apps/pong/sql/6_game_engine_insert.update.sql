
/*
INSERT INTO game_engine_in
SELECT
  ROW(
    CAST(max_proctime AS VARCHAR),
    CAST(leftY AS VARCHAR), -- lié au non détail de Row FLINK-15584
    CAST(rightY AS VARCHAR) -- lié au non détail de Row FLINK-15584
  ) AS payload
FROM global_position_history
*/



INSERT INTO game_engine_in
SELECT
  ROW(
    CAST(max_proctime AS VARCHAR),
    CAST(leftY AS VARCHAR), -- lié au non détail de Row FLINK-15584
    CAST(rightY AS VARCHAR) -- lié au non détail de Row FLINK-15584
  ) AS payload
FROM tick_service AS ts
   , LATERAL TABLE (global_position_temporal(ts.proctime_append_stream)) AS gpt
WHERE ts.constant_dummy_source = gpt.dummy_key


