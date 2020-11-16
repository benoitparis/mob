CREATE TEMPORARY VIEW write_state_mef AS
SELECT
  k,
  client_id,
  key_count,
  ts
FROM (
  SELECT 
    k,
    client_id,
    key_count,
    ts,
    ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) AS rownum
  FROM (
    SELECT
      client_id,
      payload.k AS k,
      SUM(1) key_count,
      MAX(ts) AS ts
    FROM write_state
    GROUP BY client_id, payload.k
  )
)
WHERE rownum = 1