INSERT INTO game_out_to_client
SELECT
  loopback_index,                                         
  actor_identity,
  ROW(
    CAST(gqp.proctime AS VARCHAR),
    ballX,
    ballY,
    leftY,
    rightY
  )
--FROM query_global_position_raw AS gqpf
FROM query_global_position AS gqp
--FROM query_global_position_flat AS gqpf
   , LATERAL TABLE (game_out_temporal(gqp.proctime_append_stream)) AS got
--WHERE gqpf.constant_dummy_source_hdkjshdkjsa = got.dummy_key
WHERE gqp.constant_dummy_source = got.dummy_key
--WHERE gqpf.constant_dummy = got.dummy_key
        