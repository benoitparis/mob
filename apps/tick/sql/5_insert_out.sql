INSERT INTO send_client
SELECT 
  content.client_id,
  content.payload
FROM tick_subscription_retract