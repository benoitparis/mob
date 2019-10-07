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
var insertTime = 'dsadsa';

function gameTick(inString) {
  
  var inObj = JSON.parse(inString);
  console.log(inObj);
  
  var insertTime = inObj['insert_time'];
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
  
  var out = {
    "position_timestamp" : insertTime,
    "ballX" : ballX, 
    "ballY" : ballY
  };
  
  console.log(JSON.stringify(out));
  return out;
}
