SELECT
  proctime,
  loopback_index,
  actor_identity,
  '1' constant_dummy,
  payload.side AS side
FROM query_global_position    