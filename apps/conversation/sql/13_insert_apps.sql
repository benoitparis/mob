INSERT INTO apps
SELECT
  content.client_id,
  ROW(
    app_name,
    times_asked
  )
FROM apps_available_retract