CREATE RETRACT VIEW click_count_retract AS
SELECT
  ua.client_id,
  CAST(cl.count_value AS VARCHAR) count_value
FROM user_active_v ua
JOIN (
  SELECT
    count(*) count_value
  FROM click
) cl ON true
