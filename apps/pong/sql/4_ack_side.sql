CREATE TABLE ack_side (
  client_id STRING,
  payload ROW(side STRING) 
) WITH (
  'mob.cluster-io.flow' = 'out'
)