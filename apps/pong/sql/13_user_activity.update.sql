CREATE VIEW user_activity AS
SELECT
  activity.client_id,
  sEnd IS NULL OR (CAST(tsActivity AS TIMESTAMP) > CAST(sEnd AS TIMESTAMP)) AS active
FROM (
  SELECT
    client_id,
    LAST_VALUE(CAST(ts AS VARCHAR)) AS tsActivity -- LAST_VALUE doesn't accept TIMESTAMP types
  FROM write_y
  GROUP BY client_id
) AS activity
LEFT JOIN
(
  SELECT
    client_id,
    LAST_VALUE(CAST(sEnd AS VARCHAR)) AS sEnd -- LAST_VALUE doesn't accept TIMESTAMP types
  FROM (
    SELECT
      client_id,
      SESSION_END(ts, INTERVAL '20' SECOND) sEnd
    FROM write_y
    GROUP BY client_id, SESSION(ts, INTERVAL '20' SECOND)
  )
  GROUP BY client_id
) inactivity
ON activity.client_id = inactivity.client_id