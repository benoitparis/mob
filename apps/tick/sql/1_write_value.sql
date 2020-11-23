/* +moblib (
  mob.cluster-io.flow=in
  mob.table-name=mobcatalog.tick.write_value
)
*/
CREATE TABLE write_value (
  client_id STRING,
  payload ROW(v STRING),
  ts AS localtimestamp,
  WATERMARK FOR ts AS ts
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.tick.write_value',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)
