
INSERT INTO outputTable 
SELECT
  loopback_index,
  actor_identity,
  ROW(mpTempX, mpTempY, proc_time_string) payload
FROM (
  SELECT
    loopback_index,
    actor_identity,
    mpTemp.X mpTempX, 
    mpTemp.Y mpTempY, 
    CAST(inputTable.proc_time AS VARCHAR) proc_time_string
  FROM inputTable
  JOIN LATERAL TABLE (meanPositionTemporalTable(inputTable.proc_time)) mpTemp ON mpTemp.one_key = 1
)
