
INSERT INTO outputTable 
SELECT
  loopback_index,
  actor_identity,
  ROW(mpTempX, mpTempY, proc_time_string) payload
FROM (
  SELECT
    loopback_index,
    actor_identity,
    X mpTempX, 
    Y mpTempY, 
    CAST(inputTable.proc_time AS VARCHAR) proc_time_string
  FROM inputTable
)
