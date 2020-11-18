/* +moblib (
  mob.client-io.category=input
  mob.table-name=mobcatalog.services.client_session
  mob.cluster-io.type=service
)
*/
-- TODO pré-déclarer dans les services (et puis mettre en update, remplacer par le du kafka-upsert retractant)
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