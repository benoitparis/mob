CREATE TEMPORAL TABLE FUNCTION game_out_temporal TIME ATTRIBUTE game_out_time PRIMARY KEY dummy_key AS
SELECT
  COALESCE('1', CAST(ballX AS VARCHAR)) dummy_key, -- bug no constants: Cannot add expression of different type to set
  proctime_append_stream game_out_time,
  ballX,
  ballY,
  speedX,
  speedY,
  leftY,
  rightY,
  scoreLeft,
  scoreRight
FROM game_engine_out
