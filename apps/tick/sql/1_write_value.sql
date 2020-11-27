CREATE TABLE write_value (
  client_id STRING,
  payload ROW(v STRING),
  ts AS localtimestamp,
  WATERMARK FOR ts AS ts
) WITH (
  'mob.cluster-io.flow' = 'in'
)
