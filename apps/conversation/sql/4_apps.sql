CREATE TABLE apps (
  client_id STRING,
  payload ROW(app_name STRING, times_asked STRING) 
) WITH (
  'mob.cluster-io.flow' = 'out'
)