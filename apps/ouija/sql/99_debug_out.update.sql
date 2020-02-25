INSERT INTO services.debug
SELECT
  CAST(loopback_index AS VARCHAR)
FROM query_global_position AS gqp
   , LATERAL TABLE (global_position_temporal(gqp.proctime_append_stream)) AS got
WHERE gqp.constant_dummy_source = got.dummy_key
        