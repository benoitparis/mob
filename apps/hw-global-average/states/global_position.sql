CREATE TEMPORAL TABLE global_position TIME ATTRIBUTE start_time PRIMARY KEY one_key AS
SELECT                                                                              
  loopback_index one_key,
  HOP_START(proctime, INTERVAL '0.05' SECOND, INTERVAL '5.0' SECOND) start_time, 
  MAX(X * TIMESTAMPDIFF(SECOND, proctime, HOP_PROCTIME(proctime, INTERVAL '0.05' SECOND, INTERVAL '5.0' SECOND))) avg_hop_proc_time,
  MAX(X * TIMESTAMPDIFF(SECOND, current_date, proctime)) avg_proc_time,
  --AVG(X * TIMESTAMPDIFF(SECOND, proctime, HOP_START(proctime, INTERVAL '0.05' SECOND, INTERVAL '5.0' SECOND))) avg_hop_start_time,
  AVG(X) X,
  AVG(Y) Y
FROM write_position                                                                
GROUP BY loopback_index, HOP(proctime, INTERVAL '0.05' SECOND, INTERVAL '5.0' SECOND)
/* hw-global-average: ici on va essayer de créer la dernière position de chaque client, et de faire un average global à requeter */

-- avec le AVG vs current_date il te transforme de INTEGER en INTEGER NOT NULL
-- même sans
-- integer pour un TIMESTAMPDIFF qui va pas en dessous de la SECOND c'est pas cool
--   et c'est ptet ton problème?
-- il faut rajouter du X * pour que compilation se passe bien. faudra leur faire un bugreport
-- MIN
-- 10745718
-- 10745718
-- 10745718
-- MAX 
-- 16445848
-- 16623180
-- 18038860
-- 20383580
-- 28038367
-- là avec (X+0.000001)/x c'est pas un not null, c'est un DECIMAL(1073741823, 0) -> DECIMAL(1073741823, 6); au sens où il se cale sur de la constant propagation
-- mais ça se passe dans la query du JOIN, because il te met deux timestamp avec
--   c'est une histoire d'ordering des règles? il faut le join et le réécrit, 
--   à chaque fois il y a une constante. c'est les constantes qui foutent la grouille
-- bon, avec juste notre X on devrait voir augment temps?
--  -fail: sans la constante, mais avec le cast explicite à droite: en fait toucher la droite ça change rien. 
--      pré résolution du join il a une idée du schema de la table de droite qui serait fausse, vu qu'il voit pas le cast?
-- X, sans bouger: ça fait bien les secondes (:/ basse résolution, faudra leur faire une feature request)
--   et pour notre example: HOP+PROC c'est pareil que proctime

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
