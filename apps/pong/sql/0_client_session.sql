-- TODO pré-déclarer dans les services (et puis mettre en update, remplacer par le du kafka-upsert retractant)
CREATE TABLE mobcatalog.services.client_session (
  client_id STRING,
  active    BOOLEAN
) WITH (
  'mob.cluster-io.flow' = 'in',
  'mob.cluster-io.type' = 'service'
)