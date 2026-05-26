try {
  const mqtt = require('mqtt');
  console.log('Connecting via WS (v3.1.1 forced)...');
  const client = mqtt.connect('ws://127.0.0.1:8080/mqtt', {
    clientId: 'mqttjs_test_' + Date.now(),
    clean: true,
    connectTimeout: 5000,
    protocolVersion: 4,  // MQTT 3.1.1
    wsOptions: {
      perMessageDeflate: false
    }
  });
  client.on('connect', function(connack) {
    console.log('CONNECTED! connack:', JSON.stringify(connack));
    client.end(true, function() {
      console.log('Ended');
      process.exit(0);
    });
  });
  client.on('error', function(e) {
    console.error('ERROR:', e.message);
  });
  client.on('close', function() {
    console.log('CLOSED');
    setTimeout(function() { process.exit(1); }, 100);
  });
  setTimeout(function() {
    console.log('TIMEOUT');
    process.exit(1);
  }, 8000);
} catch(e) {
  console.error('FATAL:', e);
  process.exit(1);
}
