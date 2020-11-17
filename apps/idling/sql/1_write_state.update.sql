/* +moblib (
  mob.client-io.category = input
  mob.table-name=mobcatalog.idling.write_state
)
*/
CREATE TABLE write_state (
  client_id STRING,
  payload ROW(k STRING),
  ts AS localtimestamp,
  tsProctime as PROCTIME(),
  WATERMARK FOR ts AS ts
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.idling.write_state',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)
