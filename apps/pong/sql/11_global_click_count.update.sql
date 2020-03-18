INSERT INTO click_count
SELECT
  ua.loopback_index,                                         
  ua.actor_identity,
  ROW(
    CAST(cl.count_value AS VARCHAR)
  )
FROM user_activity ua
JOIN (
  SELECT
    count(*) count_value
  FROM click
) cl ON true
WHERE ua.active