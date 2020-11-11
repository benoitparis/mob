/* +moblib (
  mob.client-io.category = input
  mob.table-name=mobcatalog.adder.query_state
)
*/
CREATE TABLE query_state (
  client_id STRING,
  proctime AS PROCTIME(),
  payload ROW(k STRING) 
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.adder.query_state',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)
