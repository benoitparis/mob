INSERT INTO game_engine_in
SELECT
  max_proctime AS insert_time,
  ROW(
    CAST(max_proctime AS VARCHAR),
    leftY,
    rightY
  ) AS payload
FROM global_position_history