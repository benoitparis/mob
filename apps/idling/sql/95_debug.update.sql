INSERT INTO services.debug
SELECT
  COALESCE(CAST('activity join with proctime' AS VARCHAR), '') || ' - ' ||
  COALESCE(CAST(client_id AS VARCHAR), '') || ' - ' ||
  COALESCE(CAST(active AS VARCHAR), '')                               
FROM (
  SELECT
    activity.client_id,
    (lastIdle IS NULL OR (lastActive  > lastIdle)) active
  FROM (
    SELECT
      client_id,
      MAX(tsProctime) AS lastActive
    FROM write_state
    GROUP BY client_id
  ) AS activity
  LEFT JOIN
  (
    SELECT
      client_id,
      MAX(lastIdle ) AS lastIdle
    FROM (
      SELECT
        client_id,
        SESSION_END(write_state.tsProctime, INTERVAL '5' SECOND) lastIdle
      FROM write_state
      GROUP BY client_id, SESSION(write_state.tsProctime, INTERVAL '5' SECOND)
    )
    GROUP BY client_id
  ) inactivity
  ON activity.client_id = inactivity.client_id
)