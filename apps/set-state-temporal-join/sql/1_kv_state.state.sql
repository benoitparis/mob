CREATE TEMPORAL TABLE FUNCTION kv_state TIME ATTRIBUTE proctime PRIMARY KEY k AS
SELECT
  k,
  proctime,
  v
FROM write_state
