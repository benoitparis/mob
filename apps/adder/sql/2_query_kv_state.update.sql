INSERT INTO send_client
SELECT
  loopback_index,                                         
  actor_identity,
  ROW(
    v
  )
FROM query_state AS qs
   , LATERAL TABLE (kv_state(gqpf.proctime)) AS kvs
WHERE qs.k = kvs.k
        