/* +moblib (
  mob.client-io.category = output
  mob.table-name=mobcatalog.fulljoin.send_client
)
*/
CREATE TABLE send_client (
  client_id STRING,
  payload ROW(v STRING)
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.fulljoin.send_client',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)