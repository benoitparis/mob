INSERT INTO game_engine_in
SELECT
  ROW(
    CAST(max_proctime AS VARCHAR),
    CAST(leftY AS VARCHAR), -- lié au non détail de Row FLINK-15584
    CAST(rightY AS VARCHAR) -- lié au non détail de Row FLINK-15584
  ) AS payload
FROM global_position_history