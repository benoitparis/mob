/* +moblib (
  mob.cluster-io.flow=in
  mob.table-name=mobcatalog.adder.write_state
)
*/
CREATE TABLE write_state (
  client_id STRING,
  payload ROW(k STRING),
  ts AS localtimestamp,
  WATERMARK FOR ts AS ts
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.adder.write_state',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)
