INSERT INTO send_client
SELECT
  qs.client_id,
  ROW(
    v
  )
FROM query_state AS qs
JOIN write_state AS ws
  ON qs.k = ws.k
        