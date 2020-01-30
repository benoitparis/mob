INSERT INTO game_out_to_client
SELECT
  loopback_index,                                         
  actor_identity,
  ROW(
    CAST(gqp.now AS VARCHAR),
    CAST(ballX AS VARCHAR),
    CAST(ballY AS VARCHAR)    
  )
FROM query_global_position AS gqp
   , LATERAL TABLE (global_position_temporal(gqp.proctime_append_stream)) AS got
WHERE gqp.constant_dummy_source = got.dummy_key
        