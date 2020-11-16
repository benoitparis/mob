/* +moblib (
  mob.client-io.category = input
  mob.table-name=mobcatalog.pong.write_y
)
*/
CREATE TABLE write_y (
  client_id STRING,
  payload ROW(y INTEGER),
  ts AS PROCTIME()
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.pong.write_y',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)