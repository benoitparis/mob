CREATE TABLE write_state (
  client_id STRING,
  payload ROW(k STRING),
  ts AS localtimestamp,
  tsProctime as PROCTIME(),
  WATERMARK FOR ts AS ts
) WITH (
  'mob.cluster-io.flow' = 'in'
)
