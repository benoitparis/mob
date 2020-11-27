CREATE TABLE game_out_to_client (
  client_id STRING,
  payload ROW(
    gameStateTime DOUBLE,
    ballX         DOUBLE,
    ballY         DOUBLE,
    speedX        DOUBLE,
    speedY        DOUBLE,
    leftY         DOUBLE,
    rightY        DOUBLE,
    scoreLeft     DOUBLE,
    scoreRight    DOUBLE
  ) 
) WITH (
  'mob.cluster-io.flow' = 'out'
)