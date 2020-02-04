
// TODO: putting wsConnect() should get logged and execute a noop
// TODO: having writeTable() in a lib, writing to the cluster directly(test), or through a managed ws (prod, with namespacing)

function writeTable(table, msg) {
  var serverMsg = {
    intent : 'WRITE',
    table : table,
    payload : msg
  };
  // console for now 
  console.log(JSON.stringify(serverMsg));
}


var internalCounter = 0;
function progress(counter) {
  
  writeTable('write_value', {
  		v : 'v' + internalCounter
  	}
  );
  internalCounter++;
  return internalCounter < 4;
  
}
