const fs = require('fs');
function log(msg) { fs.appendFileSync('mqttjs_dbg2.txt', msg + '\n'); }
log('=== START ===');
try {
  const mqtt = require('mqtt');
  log('mqtt loaded OK');
  
  // Check what WebSocket it uses
  const mqttWs = mqtt.ws || 'no ws export';
  log('mqtt.ws: ' + mqttWs);
  
  const client = mqtt.connect('ws://127.0.0.1:8080/mqtt', {
    clientId: 'test_mqttjs_' + Date.now(),
    clean: true,
    connectTimeout: 5000,
    protocolVersion: 4,
  });
  log('client created: ' + client.constructor.name);
  
  let timer = setInterval(function() {
    log('poll stream: ' + (client.stream ? client.stream.constructor.name : 'none') + 
        ' readyState=' + (client.stream ? client.stream.readyState : 'N/A'));
  }, 1000);
  
  client.on('connect', function() {
    log('MQTT CONNECTED!');
    clearInterval(timer);
    client.end(true);
  });
  client.on('error', function(e) {
    log('MQTT ERROR: ' + e.message);
  });
  client.on('close', function() {
    log('MQTT CLOSED');
  });
  
  setTimeout(function() {
    log('TIMEOUT - final stream: ' + (client.stream ? client.stream.constructor.name : 'none'));
    clearInterval(timer);
    process.exit(0);
  }, 8000);
  
} catch(e) {
  log('FATAL: ' + e.message);
  process.exit(1);
}
