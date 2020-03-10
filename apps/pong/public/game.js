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

var state = {
  side: 'observe',
  game: {
    leftY : CENTER_Y,
    rightY : CENTER_Y,
    ballX : CENTER_X,
    ballY : CENTER_Y,
    speedX : 5,
    speedY : 2,
    scoreLeft : 0,
    scoreRight : 0
  },
  user_count: {
    countLeft : 0,
    countObserve : 0,
    countRight : 0,
    countGlobal : 0
  }
}


// Specific server code: just have state, parse things, update state, produce output
var lastInsertTime = 0;

function gameTick(inString) {
  
  var inObj = JSON.parse(inString);
  //console.log(inString);
  
  var insertTime = Date.parse(inObj['insert_time'].padEnd(23, '0'));
  var deltatime;
  if (0 !== lastInsertTime) {
    deltatime = insertTime - lastInsertTime;
  } else {
    deltatime = 0;
  }
  
  state.game.leftY = parseFloat(inObj.leftY);
  state.game.rightY = parseFloat(inObj.rightY);
  
  if (0 > deltatime) {
    console.log('inString:' + JSON.stringify(inObj));
    console.log('Didnt receive updates in order with delta:' + deltatime);
    console.log('insertTime    :' + insertTime);
    console.log('lastInsertTime:' + lastInsertTime);
  }
  if (0 !== deltatime) {
    updateGame(deltatime);
  }
  lastInsertTime = insertTime;
  
  var out = {
    "position_timestamp" : insertTime.toString(),
    "ballX" : state.game.ballX, 
    "ballY" : state.game.ballY, 
    "speedX" : state.game.speedX,
    "speedY" : state.game.speedY,
    "leftY" : Math.round(state.game.leftY),  // works better with ints
    "rightY" : Math.round(state.game.rightY), 
    "scoreLeft" : state.game.scoreLeft,
    "scoreRight" : state.game.scoreRight 
  };
    
  //console.log(JSON.stringify(out));
  return out;
}

// Shared game logic
function updateGame(timeElapsedMs) {
  
  // TODO: detect collision point, then update position at point, then update speed, then update position after remaining ticking
  //   or: use a proper engine that runs on graal
  
  if (state.game.ballY - ballRadius < 0     ) {
    state.game.speedY = Math.abs(state.game.speedY);
  }
  if (state.game.ballY + ballRadius > height) {
    state.game.speedY = -Math.abs(state.game.speedY);
  }
  if ( true // left  side
    && (state.game.ballX - ballRadius < racketBorderDistance         + racketWidth /2) // right
    && (state.game.ballX + ballRadius > racketBorderDistance         - racketWidth /2) // left
    && (state.game.ballY - ballRadius < state.game.leftY             + racketHeight/2) // top
    && (state.game.ballY + ballRadius > state.game.leftY             - racketHeight/2) // bottom
  ) {
    state.game.speedX = Math.abs(state.game.speedX);
  }
  if ( true // right side
    && (state.game.ballX - ballRadius < width - racketBorderDistance + racketWidth /2) // right
    && (state.game.ballX + ballRadius > width - racketBorderDistance - racketWidth /2) // left
    && (state.game.ballY - ballRadius < state.game.rightY            + racketHeight/2) // top
    && (state.game.ballY + ballRadius > state.game.rightY            - racketHeight/2) // bottom
  ) {
    state.game.speedX = -Math.abs(state.game.speedX);
  }
  if (state.game.ballX < 0) {
    state.game.ballX = CENTER_X;
    state.game.scoreRight ++;
  }
  if (state.game.ballX > width) {
    state.game.ballX = CENTER_X;
    state.game.scoreLeft ++;
  }
  
  state.game.ballX = Math.min(state.game.ballX, width );
  state.game.ballX = Math.max(state.game.ballX, 0     );
  state.game.ballY = Math.min(state.game.ballY, height);
  state.game.ballY = Math.max(state.game.ballY, 0     );
  
  updateQuantity = timeElapsedMs / idealTickIntervalMs;
  
  state.game.ballX += state.game.speedX * updateQuantity;
  state.game.ballY += state.game.speedY * updateQuantity;
  
}