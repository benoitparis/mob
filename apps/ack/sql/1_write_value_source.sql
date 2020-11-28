CREATE TABLE write_value (
  client_id STRING,
  payload ROW(v STRING)
  --, PRIMARY KEY (client_id) NOT ENFORCED
) WITH (
  'mob.cluster-io.flow'='in'
)
