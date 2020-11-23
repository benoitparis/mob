INSERT INTO services.debug
SELECT
  COALESCE(CAST('session with proctime' AS VARCHAR), '') || ' - ' ||
  COALESCE(CAST(client_id AS VARCHAR), '') || ' - ' ||
  COALESCE(CAST(sStart AS VARCHAR), '')    || ' - ' ||
  COALESCE(CAST(sEnd AS VARCHAR), '')                                     
FROM (
  SELECT
    client_id,
    SESSION_START(write_state.tsProctime, INTERVAL '5' SECOND) sStart,
    SESSION_END(write_state.tsProctime, INTERVAL '5' SECOND) sEnd
  FROM write_state
  GROUP BY client_id, SESSION(write_state.tsProctime, INTERVAL '5' SECOND)
)