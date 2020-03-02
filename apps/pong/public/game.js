// constants
const height = 600;
const width = 1200;
const racketHeight = 100;
const racketWidth = 20;
const racketBorderDistance = 100;
const ballDiameter = 50;
const ballRadius = ballDiameter/2;
const leftDistance  = racketBorderDistance         - racketWidth/2;
const rightDistance = width - racketBorderDistance - racketWidth/2;
const idealTickIntervalMs = 20.0;
const CENTER_X = width/2;
const CENTER_Y = height/2;

// state
var leftY = CENTER_Y;
var rightY = CENTER_Y;
var ballX = CENTER_X;
var ballY = CENTER_Y;
var speedX = 5;
var speedY = 2;
var scoreLeft = 0;
var scoreRight = 0;


// Specific server code: just have state, parse things, update state, produce output
var lastInsertTime = 0;

function gameTick(inString) {
  
  var inObj = JSON.parse(inString);
  //console.log(inString);
  
  var insertTime = Date.parse(inObj['insert_time']);
  var deltatime;
  if (0 !== lastInsertTime) {
    deltatime = insertTime - lastInsertTime;
  } else {
    deltatime = 0;
  }
  lastInsertTime = insertTime;
  
  leftY = parseFloat(inObj.leftY);
  rightY = parseFloat(inObj.rightY);
  
  if (0 > deltatime) {
    console.log('Didnt receive updates in order with delta:' + deltatime);
  }
  if (0 !== deltatime) {
    updateGame(deltatime);
  }
  
  var out = {
    "position_timestamp" : insertTime.toString(),
    "ballX" : ballX, 
    "ballY" : ballY, 
    "speedX" : speedX,
    "speedY" : speedY,
    "leftY" : Math.round(leftY),  // works better with ints
    "rightY" : Math.round(rightY), 
    "scoreLeft" : scoreLeft,
    "scoreRight" : scoreRight 
  };
    
  //console.log(JSON.stringify(out));
  return out;
}

// Shared game logic
function updateGame(timeElapsedMs) {
  
  // TODO: detect collision point, then update position at point, then update speed, then update position after remaining ticking
  //   or: use a proper engine that runs on graal
  
  if (ballY < 0     ) {
    speedY = Math.abs(speedY);
  }
  if (ballY > height) {
    speedY = -Math.abs(speedY);
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
  
  ballX = Math.min(ballX, width );
  ballX = Math.max(ballX, 0     );
  ballY = Math.min(ballY, height);
  ballY = Math.max(ballY, 0     );
  
  updateQuantity = timeElapsedMs / idealTickIntervalMs;
  
  ballX += speedX * updateQuantity;
  ballY += speedY * updateQuantity;
  
}