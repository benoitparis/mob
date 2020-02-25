INSERT INTO send_client
SELECT
  qs.loopback_index,                                         
  qs.actor_identity,
  ROW(
    app_name,
    times_asked
  )
FROM (
  SELECT 
    LAST_VALUE(loopback_index) loopback_index,                                         
    LAST_VALUE(actor_identity) actor_identity,
    CAST(COUNT(*) AS VARCHAR) times_asked
  FROM query_state
) AS qs
JOIN services.app_list al
  ON true
