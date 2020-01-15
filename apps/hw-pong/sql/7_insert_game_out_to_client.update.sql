INSERT INTO game_out_to_client
SELECT
  loopback_index,                                         
  actor_identity,
  ROW(
    CAST(gqp.now AS VARCHAR),
    CAST(gqp.proctime AS VARCHAR),
    ballX,
    ballY,
    leftY,
    rightY
  )
FROM query_global_position AS gqp
   , LATERAL TABLE (game_out_temporal(gqp.proctime_append_stream)) AS got
WHERE gqp.constant_dummy_source = got.dummy_key
        