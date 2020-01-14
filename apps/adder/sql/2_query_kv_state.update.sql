INSERT INTO send_client
SELECT
  qs.loopback_index,                                         
  qs.actor_identity,
  ROW(
    CAST(key_count AS VARCHAR)
  )
FROM (SELECT loopback_index, actor_identity, k, proctime FROM query_state)                           AS qs
   , LATERAL TABLE (kv_state(qs.proctime)) AS kvs
WHERE qs.k = kvs.k
