
CREATE TABLE mobcatalog.services.client_session (
  client_id STRING,
  active    BOOLEAN
) WITH (
  'mob.cluster-io.flow' = 'in',
  'mob.cluster-io.type' = 'service'
)