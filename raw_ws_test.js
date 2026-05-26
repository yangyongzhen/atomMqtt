const WebSocket = require('ws');
console.log('starting');
const ws = new WebSocket('ws://127.0.0.1:8080/mqtt');
ws.on('open', function() {
  console.log('WS OPEN, sending MQTT CONNECT...');
  // Raw MQTT CONNECT for v3.1.1
  // Remaining Length = 6 (proto) + 1 (level) + 1 (flags) + 2 (keepalive) + 13 (client id) = 23
  const raw = Buffer.from([
    0x10, 23,
    0x00, 0x04, 0x4D, 0x51, 0x54, 0x54,  // "MQTT"
    0x04,                                   // v3.1.1
    0x02,                                   // clean session
    0x00, 0x3C,                             // keep alive 60s
    0x00, 0x0B,                             // client ID len 11
    0x74, 0x65, 0x73, 0x74, 0x5F, 0x63, 0x6C, 0x69, 0x65, 0x6E, 0x74 // "test_client"
  ]);
  console.log('Sending', raw.length, 'bytes:', raw.toString('hex'));
  ws.send(raw);
});

ws.on('message', function(data) {
  const hex = Buffer.isBuffer(data) ? Buffer.from(data).toString('hex') : data;
  console.log('WS RECEIVED:', hex);
});

ws.on('close', function(code, reason) {
  console.log('WS CLOSED code=' + code + ' reason=' + (reason || ''));
  process.exit(0);
});

ws.on('error', function(e) {
  console.error('WS ERR:', e.message);
});

setTimeout(function() {
  console.log('timeout');
  process.exit(1);
}, 5000);
