/* +moblib (
  mob.table-name=mobcatalog.pong.game_out
  mob.client-io.category=input
  mob.cluster-io.flow=in
  mob.cluster-io.type=js-engine
)
*/
CREATE TABLE game_out (
  tick_number   INTEGER,
  gameStateTime DOUBLE,
  ballX         DOUBLE,
  ballY         DOUBLE,
  speedX        DOUBLE,
  speedY        DOUBLE,
  leftY         DOUBLE,
  rightY        DOUBLE,
  scoreLeft     DOUBLE,
  scoreRight    DOUBLE
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.pong.game_out',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)