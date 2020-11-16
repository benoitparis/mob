/* +moblib (
  mob.client-io.category = output
  mob.table-name=mobcatalog.idling.send_client
)
*/
CREATE TABLE send_client (
  client_id STRING,
  payload ROW(key_count STRING)
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.idling.send_client',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)