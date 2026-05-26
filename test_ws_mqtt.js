console.log('=== MQTT-over-WS Test ===');
const mqtt = require('mqtt');
console.log('Connecting...');
const client = mqtt.connect('ws://127.0.0.1:8080/mqtt', {
  clientId: 'ws_test_' + Date.now(),
  clean: true,
  connectTimeout: 5000,
});
client.on('connect', function() {
  console.log('CONNECTED!');
  client.subscribe('test/topic', { qos: 0 }, function() {
    console.log('Subscribed to test/topic');
    client.publish('test/topic', 'Hello from WS!', { qos: 0 }, function() {
      console.log('Published message');
    });
  });
});
client.on('message', function(topic, payload) {
  console.log('MESSAGE RECEIVED:', topic, payload.toString());
  client.end(true);
});
client.on('error', function(e) {
  console.error('ERROR:', e.message);
});
client.on('close', function() {
  console.log('DISCONNECTED');
  process.exit(0);
});
setTimeout(function() {
  console.log('TIMEOUT - no response');
  client.end(true);
  process.exit(1);
}, 8000);
