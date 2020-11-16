CREATE VIEW idling_computation AS
SELECT
  sess.client_id,
  ROW(
    COALESCE(CAST(tick_number AS VARCHAR), '')
  ) payload
FROM (
  SELECT
    LAST_VALUE(tick_number) tick_number
  FROM services.tick
) ts
JOIN (
  SELECT
    activity.client_id,
    activity.tsActivity,
    inactivity.sEnd
  FROM (
    SELECT
      client_id,
      LAST_VALUE(CAST(ts AS VARCHAR)) AS tsActivity
    FROM write_state
    GROUP BY client_id
  ) AS activity
  LEFT JOIN
  (
    SELECT
      client_id,
      LAST_VALUE(CAST(sEnd AS VARCHAR)) AS sEnd
    FROM (
      SELECT
        client_id,
        SESSION_END(write_state.ts, INTERVAL '10' SECOND) sEnd
      FROM write_state
      GROUP BY client_id, SESSION(write_state.ts, INTERVAL '10' SECOND)
    )
    GROUP BY client_id
  ) inactivity
  ON activity.client_id = inactivity.client_id
) sess
ON true
WHERE true
  AND MOD(tick_number, 10) = 0
  AND (sess.sEnd IS NULL OR (CAST(tsActivity AS TIMESTAMP) > CAST(sEnd AS TIMESTAMP)))