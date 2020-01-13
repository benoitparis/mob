INSERT INTO send_client
SELECT
  loopback_index,                                         
  actor_identity,
  ROW(
    v
  )
FROM write_value wv