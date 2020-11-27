CREATE TABLE send_client (
  client_id STRING,
  payload ROW(v STRING)
) WITH (
  'mob.cluster-io.flow' = 'out'
)