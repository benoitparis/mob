CREATE RETRACT VIEW message_subscriptions_retract AS
SELECT
  qs.client_id,
  SUBSTRING(message, 1, 180) message,
  times_asked
FROM (
  SELECT 
    LAST_VALUE(client_id) client_id,
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

