INSERT INTO game_out_to_client
SELECT
  loopback_index,                                         
  actor_identity,
  ROW(
    CAST(gqpf.proctime AS VARCHAR),
    ballX,
    ballY
  )
FROM query_global_position_flat AS gqpf
   , LATERAL TABLE (game_out_temporal(gqpf.proctime)) AS got
WHERE gqpf.constant_dummy = got.dummy_key
        