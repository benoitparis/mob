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
  'mob.cluster-io.flow' = 'in',
  'mob.cluster-io.type' = 'js-engine'
)