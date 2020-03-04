INSERT INTO messages
SELECT
  qs.loopback_index,                                         
  qs.actor_identity,
  ROW(
    SUBSTRING(message, 1, 180),
    times_asked
  )
FROM (
  SELECT 
    LAST_VALUE(loopback_index) loopback_index,                                         
    LAST_VALUE(actor_identity) actor_identity,
    CAST(COUNT(*) AS VARCHAR) times_asked,
    LAST_VALUE(UNIX_TIMESTAMP()) last_asked
  FROM subscribe_messages
) AS qs
JOIN (
  SELECT
    message,
    UNIX_TIMESTAMP() message_ts
  FROM post_message
) pm
  ON true
WHERE pm.message_ts > qs.last_asked - (60 * 15)
  -- where pas trop loin dans le passé: ça serait ça la solution au lingering state?
  -- si on se débrouille bien, ça nettoierait?
