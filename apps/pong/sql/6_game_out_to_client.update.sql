/* +moblib (
  mob.client-io.category=output
  mob.table-name=mobcatalog.pong.game_out_to_client
)
*/
CREATE TABLE game_out_to_client (
  client_id STRING,
  payload ROW(
    -- DOUBLE is necessary?
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
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.pong.game_out_to_client',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)