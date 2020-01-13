CREATE TABLE game_engine
LANGUAGE js
SOURCE 'public/game.js'
IN_SCHEMA 'engine/game_in.jsonschema'
OUT_SCHEMA 'engine/game_out.jsonschema'
INVOKE 'gameTick'