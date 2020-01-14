INSERT INTO send_client
SELECT
  qs.loopback_index,                                         
  qs.actor_identity,
  ROW(
    v
  )
FROM query_state AS qs
JOIN write_state AS ws
  ON qs.k = ws.k
        