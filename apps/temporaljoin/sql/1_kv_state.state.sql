CREATE TEMPORAL TABLE FUNCTION kv_state TIME ATTRIBUTE proctime_append_stream PRIMARY KEY k AS
SELECT
  k,
  proctime_append_stream,
  v
FROM write_state
