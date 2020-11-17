INSERT INTO services.debug
SELECT client_id || ' - ' || CAST(active AS STRING)
FROM (
  SELECT
    client_id, active
  FROM (
    SELECT
      client_id, active,
      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ts DESC) rnb
    FROM (
    
      SELECT
        client_id,
        true active,
        MAX(ts) ts
      FROM write_y
      GROUP BY client_id
      
      UNION ALL
      
      SELECT
        client_id,
        false active,
        SESSION_END(ts, INTERVAL '3' SECOND) ts
      FROM write_y
      GROUP BY client_id, SESSION(ts, INTERVAL '3' SECOND)
    
    )
  )
  WHERE rnb = 1
)