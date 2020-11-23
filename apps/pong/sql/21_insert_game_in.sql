INSERT INTO game_in
SELECT
  tick_number           ,
  proctime_append_stream,
  leftY                 ,
  rightY                 
FROM game_in_retract