CREATE TABLE user_count (
  client_id STRING,
  payload ROW(
    countLeft    STRING,
    countObserve STRING,
    countRight   STRING,
    countGlobal  STRING
  ) 
) WITH (
  'mob.cluster-io.flow' = 'out'
)