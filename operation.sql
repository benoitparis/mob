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
      CAST(start_time AS VARCHAR) time_string
    FROM (
      SELECT
        loopback_index,
        actor_identity,
        AVG(X) X,
        AVG(Y) Y,
        HOP_START(proc_time, INTERVAL '0.05' SECOND, INTERVAL '5' SECOND) start_time
      FROM inputTable
      GROUP BY HOP(proc_time, INTERVAL '0.05' SECOND, INTERVAL '5' SECOND), loopback_index, actor_identity
    )
  )
)
