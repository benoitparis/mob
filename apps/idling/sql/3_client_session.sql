/* +moblib (
  mob.cluster-io.flow=in
  mob.cluster-io.type=service
  mob.table-name=mobcatalog.services.client_session
)
*/
CREATE TABLE mobcatalog.services.client_session (
  client_id STRING,
  active    BOOLEAN
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.services.client_session',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)