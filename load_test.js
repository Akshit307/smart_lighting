// load_test.js — HD version
//
// Usage:
//   node load_test.js <events> [--mode raw|batched] [--batch 25] [--wait 100] [--duration 0] [--run <id>]
//
//   --mode raw       one SQS message per event (identical to the Distinction baseline/scaled tests)
//   --mode batched   events pass through the EdgeBatcher first; one SQS message per envelope
//   --batch N        max events per envelope (batched mode only)
//   --wait MS        max edge wait before a partial envelope is flushed (batched mode only)
//   --duration S     0 = instantaneous burst (default). >0 = spread events evenly over S seconds
//   --run ID         run identifier stamped on every event (default: auto-generated)
//
// Every event carries runId + mode so latency_report.js can pull the exact stats for this run.

const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const { EdgeBatcher } = require('./edge/batcher');
const fs = require('fs');

const QUEUE_URL = 'https://sqs.ap-southeast-2.amazonaws.com/669394141948/LightingControlQueue';

// ---- args ----
const args = process.argv.slice(2);
function opt(name, def) {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] !== undefined ? args[i + 1] : def;
}
const NUM_EVENTS = parseInt(args[0]) || 50;
const MODE = opt('mode', 'raw');
const MAX_BATCH = parseInt(opt('batch', '25'));
const MAX_WAIT_MS = parseInt(opt('wait', '100'));
const DURATION_S = parseFloat(opt('duration', '0'));
const RUN_ID = opt('run', `${MODE}-${NUM_EVENTS}-${DURATION_S > 0 ? `${DURATION_S}s` : 'burst'}${MODE === 'batched' ? `-b${MAX_BATCH}` : ''}-${Date.now().toString(36)}`);

if (!['raw', 'batched'].includes(MODE)) {
  console.error('--mode must be raw or batched');
  process.exit(1);
}

const sqsClient = new SQSClient({
  region: 'ap-southeast-2',
  requestHandler: new NodeHttpHandler({
    connectionTimeout: 5000,
    socketTimeout: 5000,
    maxSockets: 5000
  })
});

let sqsMessagesSent = 0;
let sendErrors = 0;

function sendToSqs(body) {
  return sqsClient.send(new SendMessageCommand({ QueueUrl: QUEUE_URL, MessageBody: JSON.stringify(body) }))
    .then(() => { sqsMessagesSent++; })
    .catch(err => { sendErrors++; console.error('SQS send failed:', err.message); });
}

function makeEvent(i) {
  return {
    deviceId: `load_test_device_${i}`,
    propertyId: `property_${Math.floor(i / 10)}`,
    roomId: `room_${i % 10}`,
    motion: Math.random() < 0.5,
    time: Date.now(),
    runId: RUN_ID,
    mode: MODE
  };
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function run() {
  console.log(`Run ID: ${RUN_ID}`);
  console.log(`Mode: ${MODE}${MODE === 'batched' ? ` (maxBatch=${MAX_BATCH}, maxWait=${MAX_WAIT_MS}ms)` : ''}`);
  console.log(`Publishing ${NUM_EVENTS} events ${DURATION_S > 0 ? `evenly over ${DURATION_S}s` : 'as an instantaneous burst'}...`);

  // In the load test all events belong to one simulated gateway, so a single group
  // is used — this matches an edge gateway serving many devices.
  const batcher = MODE === 'batched'
    ? new EdgeBatcher({ maxBatch: MAX_BATCH, maxWaitMs: MAX_WAIT_MS, gatewayId: 'load_test_gateway', sink: (env) => sendToSqs(env) })
    : null;

  const rawSends = [];
  const startTime = Date.now();
  const intervalMs = DURATION_S > 0 ? (DURATION_S * 1000) / NUM_EVENTS : 0;

  for (let i = 0; i < NUM_EVENTS; i++) {
    if (intervalMs > 0) {
      // schedule against the start time so drift doesn't accumulate
      const target = startTime + i * intervalMs;
      const delay = target - Date.now();
      if (delay > 1) await sleep(delay);
    }
    const evt = makeEvent(i);
    if (batcher) batcher.add(evt);
    else rawSends.push(sendToSqs(evt));
  }

  if (batcher) await batcher.drain();
  await Promise.all(rawSends);

  const totalTime = Date.now() - startTime;
  const summary = {
    runId: RUN_ID,
    mode: MODE,
    events: NUM_EVENTS,
    durationTargetS: DURATION_S,
    maxBatch: MODE === 'batched' ? MAX_BATCH : 1,
    maxWaitMs: MODE === 'batched' ? MAX_WAIT_MS : 0,
    sqsMessagesSent,
    sendErrors,
    publishTimeMs: totalTime,
    eventsPerSec: +(NUM_EVENTS / (totalTime / 1000)).toFixed(1),
    batcherStats: batcher ? batcher.stats : null,
    startedAt: new Date(startTime).toISOString()
  };

  console.log(`\nDone. ${NUM_EVENTS} events -> ${sqsMessagesSent} SQS messages in ${totalTime}ms (${summary.eventsPerSec} events/sec)`);
  if (batcher) console.log('Batcher stats:', batcher.stats);
  if (sendErrors) console.log(`WARNING: ${sendErrors} SQS sends failed`);

  // keep a local log of every run for the report
  fs.mkdirSync('results', { recursive: true });
  fs.appendFileSync('results/runs.jsonl', JSON.stringify(summary) + '\n');
  console.log(`\nNext: wait ~30s for processing, then run:\n  node latency_report.js ${RUN_ID}`);
}

run();
