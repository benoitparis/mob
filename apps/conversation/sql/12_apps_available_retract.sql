CREATE RETRACT VIEW apps_available_retract AS
SELECT
  qs.client_id,
  app_name,
  times_asked
FROM (
  SELECT 
    LAST_VALUE(client_id) client_id,
    CAST(COUNT(*) AS VARCHAR) times_asked
  FROM subscribe_apps
) AS qs
JOIN services.app_list al
  ON true

