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

var CENTER_X = width/2;
var CENTER_Y = height/2;
var ballX = CENTER_X;
var ballY = CENTER_Y;
var speedX = 10;
var speedY = 5;
var insertTime = 'dsadsa';

function gameTick(inString) {
  
  var inObj = JSON.parse(inString);
  //console.log(inString);
  
  var insertTime = inObj['insert_time'];
  var leftY = inObj.leftY;
  var rightY = inObj.rightY;
  
  if (ballY < 0 || ballY > height) {
    speedY = -speedY;
  }
  if (ballX < racketBorderDistance && ballY < leftY + racketHeight/2 && ballY > leftY - racketHeight/2 ) {
    speedX = Math.abs(speedX);
  }
  if (ballX > width - racketBorderDistance && ballY < rightY + racketHeight/2 && ballY > rightY - racketHeight/2 ) {
    speedX = -Math.abs(speedX);
  }
  if (ballX < 0) {
    ballX = CENTER_X;
    // TODO score
  }
  if (ballX > width) {
    ballX = CENTER_X;
    // TODO score
  }
  
  ballX += speedX;
  ballY += speedY;
  
  var out = {
    "position_timestamp" : insertTime,
    "ballX" : ballX, 
    "ballY" : ballY, 
    "leftY" : Math.round(leftY),  // works better with ints
    "rightY" : Math.round(rightY)  
  };
  
  //console.log(JSON.stringify(out));
  return out;
}
