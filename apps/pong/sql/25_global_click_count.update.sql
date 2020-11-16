CREATE VIEW click_count_v AS
SELECT
  ua.client_id,
  CAST(cl.count_value AS VARCHAR) count_value
FROM user_activity ua
JOIN (
  SELECT
    count(*) count_value
  FROM click
) cl ON true
WHERE ua.active