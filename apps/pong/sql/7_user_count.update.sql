/* +moblib (
  mob.client-io.category=output
  mob.table-name=mobcatalog.pong.user_count
)
*/
CREATE TABLE user_count (
  client_id STRING,
  payload ROW(
    countLeft    STRING,
    countObserve STRING,
    countRight   STRING,
    countGlobal  STRING
  ) 
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.pong.user_count',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)