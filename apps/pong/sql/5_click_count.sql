CREATE TABLE click_count (
  client_id STRING,
  payload ROW(count_value STRING) 
) WITH (
  'mob.cluster-io.flow' = 'out'
)