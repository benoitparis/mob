INSERT INTO send_client
SELECT
  content.client_id,
  content.payload
FROM idling_computation_retract