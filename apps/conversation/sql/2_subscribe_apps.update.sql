/* +moblib (
  mob.client-io.category = input
  mob.table-name=mobcatalog.conversation.subscribe_apps
)
*/
CREATE TABLE subscribe_apps (
  client_id STRING
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.conversation.subscribe_apps',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)
