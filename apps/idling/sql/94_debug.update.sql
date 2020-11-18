INSERT INTO services.debug
SELECT
  COALESCE(CAST('client_session from ws' AS VARCHAR), '') || ' - ' ||
  COALESCE(CAST(client_id AS VARCHAR), '') || ' - ' ||
  COALESCE(CAST(active AS VARCHAR), '')                               
FROM mobcatalog.services.client_session