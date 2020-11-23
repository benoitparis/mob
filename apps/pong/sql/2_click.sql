/* +moblib (
  mob.cluster-io.flow=in
  mob.table-name=mobcatalog.pong.click
)
*/
CREATE TABLE click (
  client_id STRING
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.pong.click',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)