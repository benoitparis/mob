CREATE TABLE send_client (
  client_id STRING,
  payload ROW(key_count STRING)
) WITH (
  'mob.cluster-io.flow' = 'out'
)