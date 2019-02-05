-- c'est là qu'on va créer un global state à subscribe

INSERT INTO outputTable 
SELECT 
  loopback_index, actor_identity, payload
FROM (
  SELECT
    loopback_index,
    actor_identity,
    ROW(X, Y, time_string) payload
  FROM (
    SELECT
      loopback_index,
      actor_identity,
      X, 
      Y,
      CAST(proc_time AS VARCHAR) time_string
    FROM inputTable
  )
--    ROW(AVG(X), AVG(Y)) payload
--  FROM inputTable
--  GROUP BY HOP(ingest_time, INTERVAL '1' SECOND, INTERVAL '5' SECOND), loopback_index, actor_identity
)

-- talk nicely:
-- 'payload.' ~ '' ?
-- nesting
-- ROW arity > 2
