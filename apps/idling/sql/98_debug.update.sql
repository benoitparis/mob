INSERT INTO services.debug
SELECT
  COALESCE(CAST('2 second session has ended for: ' AS VARCHAR), '') ||
  COALESCE(CAST(client_id AS VARCHAR), '')                 || ' at ' ||
  COALESCE(CAST(sStart AS VARCHAR), '')
FROM (
  SELECT
    client_id,
    SESSION_END(write_state.ts, INTERVAL '2' SECOND) sEnd
  FROM write_state
  GROUP BY client_id, SESSION(write_state.ts, INTERVAL '2' SECOND)
)
