/* +moblib (
  mob.client-io.category=output
  mob.table-name=mobcatalog.conversation.apps
)
*/
CREATE TABLE apps (
  client_id STRING,
  payload ROW(app_name STRING, times_asked STRING) 
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.conversation.apps',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)