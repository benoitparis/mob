INSERT INTO send_client
SELECT
  client_id,
  ROW(
    CAST(key_count AS VARCHAR)
  )
FROM (SELECT client_id, k, proctime FROM query_state) AS qs
   , LATERAL TABLE (kv_state(qs.proctime)) AS kvs
WHERE qs.k = kvs.k
