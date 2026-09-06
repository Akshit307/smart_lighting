const mqtt = require('mqtt');
const fs = require('fs');

// --- Connect to AWS IoT Core over mutual TLS ---
const awsClient = mqtt.connect('mqtts://a2xq3lc1bxpz9m-ats.iot.ap-southeast-2.amazonaws.com:8883', {
  key: fs.readFileSync('./certs/private.pem.key'),
  cert: fs.readFileSync('./certs/device-cert.pem.crt'),
  ca: fs.readFileSync('./certs/AmazonRootCA1.pem'),
  clientId: 'property_001_living_room_hub',
  protocol: 'mqtts'
});

let awsConnected = false;

awsClient.on('connect', () => {
  console.log('[AWS IoT Core] Connected successfully');
  awsConnected = true;
});

awsClient.on('error', (err) => {
  console.error('[AWS IoT Core] Error:', err);
});

// --- Connect to local Mosquitto broker ---
const localClient = mqtt.connect('mqtt://localhost:1883');

localClient.on('connect', () => {
  console.log('[Local Mosquitto] Connected. Subscribing to smarthome/#');
  localClient.subscribe('smarthome/#');
});

localClient.on('message', (topic, message) => {
  if (!awsConnected) {
    console.log('[Bridge] AWS not connected yet, dropping message');
    return;
  }

  const awsTopic = `bridge/${topic}`;

  awsClient.publish(awsTopic, message, { qos: 1 }, (err) => {
    if (err) {
      console.error('[Bridge] Failed to publish to AWS:', err);
    } else {
      console.log(`[Bridge] Forwarded ${topic} -> AWS (${awsTopic})`);
    }
  });
});

localClient.on('error', (err) => {
  console.error('[Local Mosquitto] Error:', err);
});