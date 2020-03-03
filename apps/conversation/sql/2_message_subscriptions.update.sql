INSERT INTO messages
SELECT
  qs.loopback_index,                                         
  qs.actor_identity,
  ROW(
    message,
    times_asked
  )
FROM (
  SELECT 
    LAST_VALUE(loopback_index) loopback_index,                                         
    LAST_VALUE(actor_identity) actor_identity,
    CAST(COUNT(*) AS VARCHAR) times_asked
  FROM subscribe_messages
) AS qs
JOIN post_message al
  ON true
  -- TODO where pas trop loin dans le passé: ça serait ça la solution au lingering state?
