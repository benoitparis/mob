/* +moblib (
  mob.cluster-io.flow=in
  mob.table-name=mobcatalog.conversation.post_message
)
*/
CREATE TABLE post_message (
  client_id STRING,
  payload ROW(message STRING) 
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.conversation.post_message',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)