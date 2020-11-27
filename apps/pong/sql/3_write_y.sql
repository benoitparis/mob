CREATE TABLE write_y (
  client_id STRING,
  payload ROW(y INTEGER),
  ts AS PROCTIME()
) WITH (
  'mob.cluster-io.flow' = 'in'
)