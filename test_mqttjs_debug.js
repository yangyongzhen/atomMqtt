const mqtt = require('mqtt');
// Patch ws send to see what mqtt.js sends
const ws = require('ws');
const origSend = ws.prototype.send;
ws.prototype.send = function(data, opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = undefined; }
  if (Buffer.isBuffer(data)) {
    console.log('WS SEND raw:', data.toString('hex'));
  } else if (data instanceof ArrayBuffer) {
    console.log('WS SEND ArrayBuffer:', Buffer.from(data).toString('hex'));
  } else {
    console.log('WS SEND type:', typeof data, data);
  }
  return origSend.call(this, data, opts, cb);
};

console.log('Connecting...');
const client = mqtt.connect('ws://127.0.0.1:8080/mqtt', {
  clientId: 'mqttjs_test',
  clean: true,
  connectTimeout: 5000,
  protocolVersion: 4,
});
client.on('connect', function() {
  console.log('MQTT CONNECTED!');
  client.end();
});
client.on('error', function(e) {
  console.error('MQTT ERR:', e.message);
});
client.on('close', function() {
  console.log('MQTT CLOSED');
  process.exit(0);
});
setTimeout(function() {
  console.log('timeout');
  process.exit(1);
}, 8000);
