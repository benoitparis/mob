CREATE TABLE messages (
  client_id STRING,
  payload ROW(message STRING, times_asked STRING)
) WITH (
  'mob.cluster-io.flow' = 'out'
)