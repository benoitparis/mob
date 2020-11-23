INSERT INTO game_out_to_client
SELECT
  content.client_id,
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
  ) payload
FROM game_out_to_client_retract