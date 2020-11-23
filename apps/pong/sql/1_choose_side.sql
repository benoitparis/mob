/* +moblib (
  mob.cluster-io.flow=in
  mob.table-name=mobcatalog.pong.choose_side
)
*/
CREATE TABLE choose_side (
  client_id STRING,
  payload ROW(side STRING) 
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.pong.choose_side',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)