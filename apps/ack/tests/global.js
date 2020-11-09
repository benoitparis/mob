// lib part
// TODO: putting wsConnect() should get logged and execute a noop
// TODO: having writeTable() in a lib, writing to the cluster directly(test), or through a managed ws (prod, with namespacing)
// TODO: having the name of the app in writeTable for the target table

var wsQueueOut = [];
function writeTable(table, msg) {
  var serverMsg = {
    intent : 'WRITE',
    table : 'ack.' + table,
    payload : msg
  };
  // console for now 
  console.log(JSON.stringify(serverMsg));
  wsQueueOut.push(JSON.stringify(serverMsg));
}

function toServer() {
  return wsQueueOut.shift();
}
///////////////////////////////////////////////// begin test

var internalCounter = 0;
function progress(counter) {
  
  console.log("progressed, counter:" + counter + ", internalCounter: " + internalCounter);
  writeTable('write_value', {
  		v : 'v' + internalCounter
  	}
  );
  internalCounter++;
  return internalCounter < 4;
  
}

var wsQueueIn = [];
function fromServer(data) {
  wsQueueIn.push(JSON.parse(data));
}
function validate() {
  console.log(JSON.stringify(wsQueueIn));
  var wsQueueInString = wsQueueIn.map(it => JSON.stringify(it));
  var okMessage0 = wsQueueInString.includes(JSON.stringify({ "table" : "send_client", "payload" : {"v":"v0"}}));
  var okMessage1 = wsQueueInString.includes(JSON.stringify({ "table" : "send_client", "payload" : {"v":"v1"}}));
  var okMessage2 = wsQueueInString.includes(JSON.stringify({ "table" : "send_client", "payload" : {"v":"v2"}}));
  var okMessage3 = wsQueueInString.includes(JSON.stringify({ "table" : "send_client", "payload" : {"v":"v3"}}));
  console.log("okMessage0: " + okMessage0);
  console.log("okMessage1: " + okMessage1);
  console.log("okMessage2: " + okMessage2);
  console.log("okMessage3: " + okMessage3);
  return true 
    && okMessage0
    && okMessage1
    && okMessage2
    && okMessage3;
}

///////////////////////////////////////////////// end test
// lib part
var exports = {
  "progress" : progress,
  "toServer" : toServer,
  "fromServer" : fromServer,
  "validate" : validate
}
exports;