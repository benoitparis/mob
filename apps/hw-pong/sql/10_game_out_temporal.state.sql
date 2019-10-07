CREATE TEMPORAL TABLE game_out_temporal TIME ATTRIBUTE game_out_time PRIMARY KEY dummy_key AS
SELECT
  COALESCE('1', CAST(ballX AS VARCHAR)) dummy_key,
  proctime game_out_time,
  ballX,
  ballY,
  leftY,
  rightY
FROM game_engine_out
