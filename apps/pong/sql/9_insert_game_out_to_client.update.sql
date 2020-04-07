INSERT INTO game_out_to_client
SELECT
  loopback_index,                                         
  actor_identity,
  ROW(
    gameStateTime,
    ballX        ,
    ballY        ,
    speedX       ,
    speedY       ,
    leftY        ,
    rightY       ,
    scoreLeft    ,
    scoreRight   
  )
FROM (
  SELECT
    LAST_VALUE(gameStateTime) AS gameStateTime,
    LAST_VALUE(ballX        ) AS ballX        ,
    LAST_VALUE(ballY        ) AS ballY        ,
    LAST_VALUE(speedX       ) AS speedX       ,
    LAST_VALUE(speedY       ) AS speedY       ,
    LAST_VALUE(leftY        ) AS leftY        ,
    LAST_VALUE(rightY       ) AS rightY       ,
    LAST_VALUE(scoreLeft    ) AS scoreLeft    ,
    LAST_VALUE(scoreRight   ) AS scoreRight   
  FROM game_engine_out
  WHERE MOD(tick_number, 1) = 0
) geo_last
JOIN user_activity ua ON true
WHERE ua.active
