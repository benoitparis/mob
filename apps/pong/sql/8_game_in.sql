CREATE TABLE game_in (
  tick_number STRING,
  insert_time STRING,
  leftY       STRING,
  rightY      STRING
) WITH (
  'mob.cluster-io.flow' = 'out',
  'mob.cluster-io.type' = 'js-engine',
  'mob.js-engine.code' = 'public/game.js',
  'mob.js-engine.invoke-function' = 'gameTick'
)