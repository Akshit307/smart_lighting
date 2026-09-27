const mqtt = require('mqtt');
const fs = require('fs');
const { EdgeBatcher } = require('./edge/batcher');

// --- Config ---
// BATCHING=off  -> original behaviour: every local message forwarded to AWS individually
// BATCHING=on   -> (default) events are aggregated at the edge into batch envelopes
const BATCHING = (process.env.BATCHING || 'on').toLowerCase() !== 'off';
const MAX_BATCH = parseInt(process.env.MAX_BATCH) || 25;
const MAX_WAIT_MS = parseInt(process.env.MAX_WAIT_MS) || 100;
const GATEWAY_ID = 'property_001_living_room_hub';

// --- Connect to AWS IoT Core over mutual TLS ---
const awsClient = mqtt.connect('mqtts://a2xq3lc1bxpz9m-ats.iot.ap-southeast-2.amazonaws.com:8883', {
  key: fs.readFileSync('./certs/private.pem.key'),
  cert: fs.readFileSync('./certs/device-cert.pem.crt'),
  ca: fs.readFileSync('./certs/AmazonRootCA1.pem'),
  clientId: GATEWAY_ID,
  protocol: 'mqtts'
});

let awsConnected = false;

awsClient.on('connect', () => {
  console.log(`[AWS IoT Core] Connected successfully (batching: ${BATCHING ? `ON, maxBatch=${MAX_BATCH}, maxWait=${MAX_WAIT_MS}ms` : 'OFF'})`);
  awsConnected = true;
});

awsClient.on('error', (err) => {
  console.error('[AWS IoT Core] Error:', err);
});

// --- Edge batcher: one envelope per property, published to bridge/smarthome/<propertyId>/batch ---
const batcher = new EdgeBatcher({
  maxBatch: MAX_BATCH,
  maxWaitMs: MAX_WAIT_MS,
  gatewayId: GATEWAY_ID,
  groupBy: (evt) => evt.propertyId || 'unknown',
  // manual switch overrides are user-facing, so they skip the wait window
  isUrgent: (evt) => evt.overrideState !== undefined,
  sink: (envelope, propertyId) => new Promise((resolve, reject) => {
    const awsTopic = `bridge/smarthome/${propertyId}/batch`;
    awsClient.publish(awsTopic, JSON.stringify(envelope), { qos: 1 }, (err) => {
      if (err) return reject(err);
      console.log(`[Bridge] Envelope #${envelope.seq} (${envelope.count} events, ${envelope.flushReason} flush) -> AWS (${awsTopic})`);
      resolve();
    });
  })
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

  if (BATCHING) {
    let evt;
    try {
      evt = JSON.parse(message.toString());
    } catch (e) {
      console.error(`[Bridge] Skipping non-JSON message on ${topic}`);
      return;
    }
    if (!evt.propertyId) evt.propertyId = topic.split('/')[1];
    batcher.add(evt);
    return;
  }

  // Original un-batched path
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

// Flush anything buffered before exiting (Ctrl+C)
process.on('SIGINT', async () => {
  console.log('\n[Bridge] Shutting down, flushing edge buffer...');
  await batcher.drain();
  console.log('[Bridge] Batcher stats:', batcher.stats);
  process.exit(0);
});
