CREATE TEMPORAL TABLE FUNCTION write_state_ttf TIME ATTRIBUTE ts PRIMARY KEY k AS
SELECT
  k,
  COALESCE(LAST_VALUE(ts), null) ts,
  COALESCE(SUM(1), null) key_count  -- workaround bug "Cannot add expression of different type to set"
FROM write_state
GROUP BY k