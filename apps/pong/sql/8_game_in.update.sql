/* +moblib (
  mob.table-name=mobcatalog.pong.game_in
  mob.client-io.category=output
  mob.cluster-io.flow=out
  mob.cluster-io.type=js-engine
  mob.cluster-io.js-engine.code=public/game.js
  mob.cluster-io.js-engine.invoke-function=gameTick
)
*/
CREATE TABLE game_in (
  payload ROW(
    tick_number STRING,
    insert_time STRING,
    leftY       STRING,
    rightY      STRING
  ) 
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.property-version' = '1',
  'connector.topic' = 'mobcatalog.pong.game_in',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'format.type' = 'json'
)