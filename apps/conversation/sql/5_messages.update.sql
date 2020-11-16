/* +moblib (
  mob.client-io.category=output
  mob.table-name=mobcatalog.conversation.messages
)
*/
CREATE TABLE messages (
  client_id STRING,
  payload ROW(message STRING, times_asked STRING)
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.conversation.messages',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)