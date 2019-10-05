INSERT INTO game_out_to_client
SELECT
  1 AS loopback_index,                                         
  'plop' AS actor_identity,
  ROW(
    position_timestamp,
    ballX,
    ballY
  )
FROM game_engine_out
