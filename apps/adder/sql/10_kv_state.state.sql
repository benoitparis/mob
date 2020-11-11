CREATE TEMPORAL TABLE FUNCTION kv_state TIME ATTRIBUTE last_proctime PRIMARY KEY k AS
SELECT
  k,
  --LAST_VALUE(proctime) last_proctime,
  proctime last_proctime,
  COALESCE(1, null) key_count  -- workaround bug "Cannot add expression of different type to set"
FROM write_state
--GROUP BY k
