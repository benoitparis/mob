CREATE TABLE write_value (
  client_id STRING,
  payload ROW(v STRING) 
) WITH (
  'mob.cluster-io.flow'='in'
)
