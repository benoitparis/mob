// begin from index.html
var height = 600;
var width = 1200;
var racketHeight = 100;
var racketWidth = 20;
var racketBorderDistance = 100;
var ballDiameter = 50;
var ballRadius = ballDiameter/2;
var leftDistance  = racketBorderDistance         - racketWidth/2;
var rightDistance = width - racketBorderDistance - racketWidth/2;
// end from index.html

var CENTER_X = width/2;
var CENTER_Y = height/2;
var ballX = CENTER_X;
var ballY = CENTER_Y;
var speedX = 5;
var speedY = 2;
var lastInsertTime = new Date();
var scoreLeft = 0;
var scoreRight = 0;

function gameTick(inString) {
  
  var inObj = JSON.parse(inString);
  //console.log(inString);
  
  var insertTime = Date.parse(inObj['insert_time']);
  var deltatime = insertTime - lastInsertTime;
  
  var leftY = parseFloat(inObj.leftY);
  var rightY = parseFloat(inObj.rightY);
  
  if (ballY < 0 || ballY > height) {
    speedY = -speedY;
  }
  if ( true // left  side
    && (ballX - ballRadius < racketBorderDistance         + racketWidth /2) // right
    && (ballX + ballRadius > racketBorderDistance         - racketWidth /2) // left
    && (ballY - ballRadius < leftY                        + racketHeight/2) // top
    && (ballY + ballRadius > leftY                        - racketHeight/2) // bottom
  ) {
    speedX = Math.abs(speedX);
  }
  if ( true // right side
    && (ballX - ballRadius < width - racketBorderDistance + racketWidth /2) // right
    && (ballX + ballRadius > width - racketBorderDistance - racketWidth /2) // left
    && (ballY - ballRadius < rightY                       + racketHeight/2) // top
    && (ballY + ballRadius > rightY                       - racketHeight/2) // bottom
  ) {
    speedX = -Math.abs(speedX);
  }
  if (ballX < 0) {
    ballX = CENTER_X;
    scoreRight ++;
  }
  if (ballX > width) {
    ballX = CENTER_X;
    scoreLeft ++;
  }
  
  ballX += speedX;
  ballY += speedY;
  
  var out = {
    "position_timestamp" : insertTime.toString(),
    "ballX" : ballX, 
    "ballY" : ballY, 
    "leftY" : Math.round(leftY),  // works better with ints
    "rightY" : Math.round(rightY), 
    "scoreLeft" : scoreLeft,
    "scoreRight" : scoreRight 
  };
  
  lastInsertTime = insertTime;
  
  //console.log(JSON.stringify(out));
  return out;
}












































































