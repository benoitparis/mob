/* +moblib (
  mob.cluster-io.flow=out
  mob.table-name=mobcatalog.pong.ack_side
)
*/
CREATE TABLE ack_side (
  client_id STRING,
  payload ROW(side STRING) 
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.pong.ack_side',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)