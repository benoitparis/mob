INSERT INTO services.debug
SELECT
  CAST(client_id   AS VARCHAR) || ' - ' ||
  CAST(app_name    AS VARCHAR) || ' - ' ||
  CAST(times_asked AS VARCHAR) 
FROM apps_available_retract
