INSERT INTO send_client
SELECT
  qs.loopback_index,                                         
  qs.actor_identity,
  ROW(
    v
  )
FROM query_state AS qs
   , LATERAL TABLE (kv_state(qs.proctime_append_stream)) AS kvs
WHERE qs.k = kvs.k
