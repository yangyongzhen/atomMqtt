const fs = require('fs');
function log(m) { console.log(m); }
try {
  const WebSocket = globalThis.WebSocket;
  log('Native WS exists: ' + !!WebSocket);
  log('Native WS name: ' + (WebSocket ? WebSocket.name : 'N/A'));
  
  if (WebSocket) {
    log('Connecting with native WS...');
    const ws = new WebSocket('ws://127.0.0.1:8080/mqtt');
    ws.onopen = function() { log('WS OPEN (native)'); ws.close(); };
    ws.onclose = function() { log('WS CLOSED'); process.exit(0); };
    ws.onerror = function(e) { log('WS ERROR: ' + e.message); };
  }
  
  setTimeout(function() { log('TIMEOUT'); process.exit(1); }, 5000);
} catch(e) {
  log('FATAL: ' + e.message);
  process.exit(1);
}
