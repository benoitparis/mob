/* +moblib (
  mob.cluster-io.flow=out
  mob.table-name=mobcatalog.pong.click_count
)
*/
CREATE TABLE click_count (
  client_id STRING,
  payload ROW(count_value STRING) 
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.pong.click_count',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)