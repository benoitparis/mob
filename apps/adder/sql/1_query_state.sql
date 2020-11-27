CREATE TABLE query_state (
  client_id STRING,
  payload ROW(k STRING),
  ts AS localtimestamp,
  WATERMARK FOR ts AS ts
) WITH (
  'mob.cluster-io.flow' = 'in'
)
