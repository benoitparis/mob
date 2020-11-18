CREATE VIEW user_active_v AS
  /* TODO remplacer par le du kafka-upsert retractant */
SELECT
  client_id
FROM (
  SELECT
    client_id,
    LAST_VALUE(active) active
  FROM mobcatalog.services.client_session
  GROUP BY client_id
)
WHERE active