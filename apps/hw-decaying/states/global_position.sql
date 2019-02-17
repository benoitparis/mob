CREATE TEMPORAL TABLE global_position TIME ATTRIBUTE start_time PRIMARY KEY one_key AS
SELECT                                                                              
  loopback_index one_key,                                                           
  HOP_START(proctime, INTERVAL '0.05' SECOND, INTERVAL '1.0' SECOND) start_time,
  AVG(X) X,
  AVG(Y) Y
FROM write_position                                                                
GROUP BY loopback_index, HOP(proctime, INTERVAL '0.05' SECOND, INTERVAL '1.0' SECOND)

/* hw-decaying: ici on va tester le windowing pour faire du cedaying average sur un window, par index de loopback */



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
