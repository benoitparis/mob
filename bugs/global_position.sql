CREATE TEMPORAL TABLE global_position TIME ATTRIBUTE start_time PRIMARY KEY one_key AS
/*
SELECT                                                                              
  loopback_index one_key,                                                           
  HOP_START(proctime, INTERVAL '0.05' SECOND, INTERVAL '1.0' SECOND) start_time,
  AVG(X) X,
  AVG(Y) Y
FROM write_position                                                                
GROUP BY loopback_index, HOP(proctime, INTERVAL '0.05' SECOND, INTERVAL '1.0' SECOND)
*/
/*
SELECT 
  one_key,
  MAX(start_time) OVER w AS start_time,
  --start_time,
  X,
  Y,
  Y data1
  --AVG(one_key) OVER w AS one_key,
  --FIRST_VALUE(one_key) OVER w, 
  --FIRST_VALUE(start_time) OVER w,
  --AVG(X) OVER w AS X,
  --AVG(Y) OVER w AS Y
FROM (
  SELECT                                
    loopback_index one_key,
    TUMBLE_START(proctime, INTERVAL '0.05' SECOND) start_time,
    AVG(X) X,
    AVG(Y) Y
  FROM write_position
  GROUP BY loopback_index, TUMBLE(proctime, INTERVAL '0.05' SECOND)
) 
WINDOW w AS (
  PARTITION BY one_key
  ORDER BY start_time
  ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)
-- TableException: OVER windows can only be applied on time attributes.
*/

SELECT
  one_key,
  start_time,
  CAST(
  MAX(TIMESTAMPDIFF(
    SECOND, 
    proctime, 
    start_time
  )) OVER w2 AS DECIMAL) data1,
  CAST(
    SUM(X   * EXP(-TIMESTAMPDIFF(
      SECOND, 
      proctime, 
      start_time))
    ) OVER w2
    / 
    SUM(1.0 * EXP(-TIMESTAMPDIFF(
      SECOND, 
      proctime, 
      start_time))
    ) OVER w2
  AS DECIMAL) AS X,
  AVG(Y) OVER w2 AS Y
FROM (
  SELECT
    loopback_index one_key,
    proctime,
    MIN(proctime) OVER w1 AS start_time,
    X,
    Y
  FROM write_position
  WINDOW w1 AS (
    PARTITION BY loopback_index
    ORDER BY proctime
    ROWS BETWEEN 50 PRECEDING AND CURRENT ROW
  )
)
WINDOW w2 AS (
  PARTITION BY one_key
  ORDER BY start_time
  ROWS BETWEEN 50 PRECEDING AND CURRENT ROW
)
-- a marche pas. ça doit prendre le MIN(proctime) comme un MIN(<tu fera system.gettime>).
--   il faut que tu choppes le ingestiontime (timecharacteristic c'est bien le ingestiontime!)


/* hw-decaying: ici on va tester le windowing pour faire du decaying average sur un window, par index de loopback */



-- avec le MAX:
-- Exception in thread "main" org.apache.flink.table.api.ValidationException: SQL validation failed. From line 7, column 3 to line 7, column 187: 
--   Aggregate expressions cannot be nested
-- et on a pas accès à HOP_START

--MAX(HOP_PROCTIME(proctime, INTERVAL '0.05' SECOND, INTERVAL '5' SECOND)) start_time,
--MIN(HOP_PROCTIME(proctime, INTERVAL '0.05' SECOND, INTERVAL '5' SECOND)) hop_proctime,
-- donne le start et le end
--  -> donc ça varie!
--  .. probablement == proctime. et EXP(0) * X = constante
--  pourquoi on a pas le hop_start?
-- idem pour                                                 
--MAX(proctime) start_time,
--MIN(proctime) hop_proctime,
-- au moins c'est cohérent
-- ça c'est pas mal:
--MAX(HOP_PROCTIME(proctime, INTERVAL '0.05' SECOND, INTERVAL '5' SECOND)) start_time,
--HOP_PROCTIME(proctime, INTERVAL '0.05' SECOND, INTERVAL '5' SECOND) hop_proctime,
-- on a le max hop < hop
-- -> c'est vraiment deux notions différentes
-- idée de debug: lire cette table directement?
-- 
