INSERT INTO services.debug
SELECT 
  CAST(gameStateTime AS STRING) || ' - ' || 
  CAST(ballX         AS STRING) || ' - ' || 
  CAST(ballY         AS STRING) || ' - ' || 
  CAST(speedX        AS STRING) || ' - ' || 
  CAST(speedY        AS STRING) || ' - ' || 
  CAST(leftY         AS STRING) || ' - ' || 
  CAST(rightY        AS STRING) || ' - ' || 
  CAST(scoreLeft     AS STRING) || ' - ' || 
  CAST(scoreRight    AS STRING) || ' - ' || 
  CAST(content.client_id        AS STRING) 
FROM game_out_to_client_retract
