// from index.html
var height = 600;
var width = 1200;
var racketHeight = 100;
var racketWidth = 20;
var racketBorderDistance = 100;
var ballDiameter = 50;
var ballRadius = ballDiameter/2;
var leftDistance  = racketBorderDistance         - racketWidth/2;
var rightDistance = width - racketBorderDistance - racketWidth/2;


var ballX = 300;
var ballY = 300;
var speedX = 20;
var speedY = 20;

function gameTick(inObj) {
  
  var watermark = inObj.watermark;
  var leftY = inObj.leftY;
  var rightY = inObj.rightY;
  
  if (ballY < 0 || ballY > height) {
    speedY = -speedY;
  }
  if (ballX < racketBorderDistance && ballY < leftY + racketHeight/2 && ballY > leftY - racketHeight/2 ) {
    speedX = -speedX;
  }
  if (ballX > width - racketBorderDistance && ballY < rightY + racketHeight/2 && ballY > rightY - racketHeight/2 ) {
    speedX = -speedX;
  }
  
  ballX += speedX;
  ballY += speedY;
  
  return {
    "watermarkIn" : watermark,
    "ballX" : ballX, 
    "ballY" : ballY
  }; 
}
