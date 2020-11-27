CREATE TABLE post_message (
  client_id STRING,
  payload ROW(message STRING) 
) WITH (
  'mob.cluster-io.flow' = 'in'
)