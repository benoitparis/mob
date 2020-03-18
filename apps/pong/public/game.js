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
const INIT_SPEED_X = 5;
const INIT_SPEED_Y = 2;

var state = {
  side: 'observe',
  game: {
    leftY : CENTER_Y,
    rightY : CENTER_Y,
    ballX : CENTER_X,
    ballY : CENTER_Y,
    speedX : INIT_SPEED_X,
    speedY : INIT_SPEED_Y,
    scoreLeft : 0,
    scoreRight : 0
  },
  user_count: {
    countLeft : 0,
    countObserve : 0,
    countRight : 0,
    countGlobal : 0
  },
  click_count: {
    global : 0
  }
}

// credits https://gist.github.com/mathiasbynens/5670917
prng = (function() {
  // TODO share seed client-side, for interpolation sync
  var seed = 0x2F6E2B1;
	return function() {
		// Robert Jenkins’ 32 bit integer hash function
		seed = ((seed + 0x7ED55D16) + (seed << 12))  & 0xFFFFFFFF;
		seed = ((seed ^ 0xC761C23C) ^ (seed >>> 19)) & 0xFFFFFFFF;
		seed = ((seed + 0x165667B1) + (seed << 5))   & 0xFFFFFFFF;
		seed = ((seed + 0xD3A2646C) ^ (seed << 9))   & 0xFFFFFFFF;
		seed = ((seed + 0xFD7046C5) + (seed << 3))   & 0xFFFFFFFF;
		seed = ((seed ^ 0xB55A4F09) ^ (seed >>> 16)) & 0xFFFFFFFF;
		return (seed & 0xFFFFFFF) / 0x10000000;
	};
}());

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
  
  if (state.game.ballY - ballRadius <= 0     ) {
    state.game.speedY = Math.abs(state.game.speedY);
  }
  if (state.game.ballY + ballRadius >= height) {
    state.game.speedY = -Math.abs(state.game.speedY);
  }
  if ( true // left  side
    && (state.game.ballX - (        racketBorderDistance) < 0 + ballRadius + racketWidth /2) // right
    && (state.game.ballX - (        racketBorderDistance) > 0 - ballRadius - racketWidth /2) // left
    && (state.game.ballY -          state.game.leftY      < 0 + ballRadius + racketHeight/2) // top
    && (state.game.ballY -          state.game.leftY      > 0 - ballRadius - racketHeight/2) // bottom
  ) {
    var randomX = Math.floor(prng() * 3) - 1;
    state.game.speedX = Math.abs(INIT_SPEED_X) + randomX;
    var accY = Math.floor((
      ((state.game.ballY - state.game.leftY) + ballRadius + racketHeight/2) / 
      (ballRadius*2 + racketHeight)
    ) * 3) - 1;
    state.game.speedY = state.game.speedY + accY;
  }
  if ( true // right side
    && (state.game.ballX - (width - racketBorderDistance) < 0 + ballRadius + racketWidth /2) // right
    && (state.game.ballX - (width - racketBorderDistance) > 0 - ballRadius - racketWidth /2) // left
    && (state.game.ballY -          state.game.rightY     < 0 + ballRadius + racketHeight/2) // top
    && (state.game.ballY -          state.game.rightY     > 0 - ballRadius - racketHeight/2) // bottom
  ) {
    var randomX = Math.floor(prng() * 3) - 1;
    state.game.speedX = -Math.abs(INIT_SPEED_X) + randomX;
    var accY = Math.floor((
      ((state.game.ballY - state.game.rightY) + ballRadius + racketHeight/2) / 
      (ballRadius*2 + racketHeight)
    ) * 3) - 1;
    state.game.speedY = state.game.speedY + accY;
  }
  if (state.game.ballX <= 0) {
    state.game.ballX = CENTER_X;
    state.game.scoreRight ++;
  }
  if (state.game.ballX >= width) {
    state.game.ballX = CENTER_X;
    state.game.scoreLeft ++;
  }
  if (state.game.scoreLeft > 25 || state.game.scoreRight > 25) {
    state.game.scoreLeft = 0;
    state.game.scoreRight = 0;
  }
  
  state.game.ballX = Math.min(state.game.ballX, width );
  state.game.ballX = Math.max(state.game.ballX, 0     );
  state.game.ballY = Math.min(state.game.ballY, height);
  state.game.ballY = Math.max(state.game.ballY, 0     );
  
  updateQuantity = timeElapsedMs / idealTickIntervalMs;
  
  state.game.ballX += state.game.speedX * updateQuantity;
  state.game.ballY += state.game.speedY * updateQuantity;
  
}