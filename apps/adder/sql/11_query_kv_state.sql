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
LEFT JOIN write_state_mef FOR SYSTEM_TIME AS OF qs.ts AS kvs
  ON kvs.k = qs.k
