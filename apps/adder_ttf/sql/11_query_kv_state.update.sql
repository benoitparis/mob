INSERT INTO send_client
SELECT
  qs.client_id,
  ROW(
    CAST(kvs.key_count AS VARCHAR)
  )
FROM (
  SELECT
    client_id,
    payload.k AS k,
    ts
  FROM query_state
) AS qs
, LATERAL TABLE (write_state_ttf(qs.ts)) AS kvs
WHERE qs.k = kvs.k
