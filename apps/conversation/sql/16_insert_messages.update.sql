INSERT INTO messages
SELECT
  content.client_id,
  ROW (
    message,
    times_asked
  )
FROM message_subscriptions_retract