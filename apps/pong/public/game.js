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
    gameStateTime : null,
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
const prng = (function() {
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
var lastUpdateTime = new Date().getTime();
function gameTick(inString) {
  
  var inObj = JSON.parse(inString);
  //console.log(inString);
  
  //var insertTime = Date.parse(inObj['insert_time'].padEnd(23, '0')); // bug graaljs?
  var now = new Date().getTime();
  var deltatime;
  if (0 !== lastUpdateTime) {
    deltatime = now - lastUpdateTime;
  } else {
    deltatime = 0;
  }
  
  state.game.leftY = Math.round(parseFloat(inObj.leftY));
  state.game.rightY = Math.round(parseFloat(inObj.rightY));

  if (60 < deltatime) {
    console.log('Large deltatime, inString:' + JSON.stringify(inObj));
    console.log('Large deltatime, deltatime:' + deltatime);
  }
  if (0 !== deltatime) {
    state.game = updateGame(state.game, deltatime);
  }
  lastUpdateTime = now;
  
  // DONE pas le state, mais bon..
  state.game.tick_number = parseInt(inObj['tick_number'], 10);
  state.game.gameStateTime = now;
  
  //console.log('return:'+JSON.stringify(state.game));
  return state.game;
}

// Shared game logic
function updateGame(gameIn, timeElapsedMs) {
  var game = Object.assign({}, gameIn); // bug graaljs?
  
  // TODO: detect collision point, then update position at point, then update speed, then update position after remaining ticking
  //   or: use a proper engine that runs on graal
  
  if (game.ballY - ballRadius <= 0     ) {
    game.speedY = Math.abs(game.speedY);
  }
  if (game.ballY + ballRadius >= height) {
    game.speedY = -Math.abs(game.speedY);
  }
  if ( true // left  side
    && (game.ballX - (        racketBorderDistance) < 0 + ballRadius + racketWidth /2) // right
    && (game.ballX - (        racketBorderDistance) > 0 - ballRadius - racketWidth /2) // left
    && (game.ballY -          game.leftY      < 0 + ballRadius + racketHeight/2) // top
    && (game.ballY -          game.leftY      > 0 - ballRadius - racketHeight/2) // bottom
  ) {
    var randomX = Math.floor(prng() * 3) - 1;
    game.speedX = Math.abs(INIT_SPEED_X) + randomX;
    var accY = Math.floor((
      ((game.ballY - game.leftY) + ballRadius + racketHeight/2) / 
      (ballRadius*2 + racketHeight)
    ) * 3) - 1;
    game.speedY = game.speedY + accY;
  }
  if ( true // right side
    && (game.ballX - (width - racketBorderDistance) < 0 + ballRadius + racketWidth /2) // right
    && (game.ballX - (width - racketBorderDistance) > 0 - ballRadius - racketWidth /2) // left
    && (game.ballY -          game.rightY     < 0 + ballRadius + racketHeight/2) // top
    && (game.ballY -          game.rightY     > 0 - ballRadius - racketHeight/2) // bottom
  ) {
    var randomX = Math.floor(prng() * 3) - 1;
    game.speedX = -Math.abs(INIT_SPEED_X) + randomX;
    var accY = Math.floor((
      ((game.ballY - game.rightY) + ballRadius + racketHeight/2) / 
      (ballRadius*2 + racketHeight)
    ) * 3) - 1;
    game.speedY = game.speedY + accY;
  }
  if (game.ballX <= 0) {
    game.ballX = CENTER_X;
    game.ballY = CENTER_Y;
    game.scoreRight ++;
  }
  if (game.ballX >= width) {
    game.ballX = CENTER_X;
    game.ballY = CENTER_Y;
    game.scoreLeft ++;
  }
  if (game.scoreLeft > 25 || game.scoreRight > 25) {
    game.scoreLeft = 0;
    game.scoreRight = 0;
  }
  
  
  
  game.ballX = Math.min(game.ballX, width );
  game.ballX = Math.max(game.ballX, 0     );
  game.ballY = Math.min(game.ballY, height);
  game.ballY = Math.max(game.ballY, 0     );
  
  updateQuantity = timeElapsedMs / idealTickIntervalMs;

  game.ballX += game.speedX * updateQuantity;
  game.ballY += game.speedY * updateQuantity;
  //console.log('game:'+game);
  return game;
}