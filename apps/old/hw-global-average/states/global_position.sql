CREATE TEMPORAL TABLE global_position TIME ATTRIBUTE max_proctime PRIMARY KEY loopback_index AS
SELECT                                                                              
  loopback_index,
  MAX(max_proctime) max_proctime,
  AVG(X) X,
  AVG(Y) Y
FROM (
  SELECT
    loopback_index,
    actor_identity,
    MAX(proctime) max_proctime,
    LAST_VALUE(X) X,
    LAST_VALUE(Y) Y
  FROM write_position
  GROUP BY loopback_index, actor_identity
) tbl
GROUP BY loopback_index



