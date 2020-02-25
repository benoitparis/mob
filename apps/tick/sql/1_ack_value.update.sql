INSERT INTO send_client
SELECT
  wv.loopback_index,                                         
  wv.actor_identity,
  ROW(
    CAST(ts.tick_number AS VARCHAR)
  )
FROM write_value AS wv
JOIN services.tick AS ts ON true