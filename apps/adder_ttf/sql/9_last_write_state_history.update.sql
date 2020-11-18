CREATE VIEW last_write_state_history AS
SELECT
  k,
  COALESCE(LAST_VALUE(ts), null) ts,
  COALESCE(SUM(1), null) key_count  -- workaround bug "Cannot add expression of different type to set"
FROM write_state
GROUP BY k