// run_experiments.js
// Runs the full HD experiment matrix back-to-back and collects results.
//
// Usage:  node run_experiments.js            (full matrix, ~25-30 min)
//         node run_experiments.js --quick    (one raw + one batched 5000 burst, ~3 min — sanity check)
//         node run_experiments.js --knee     (1500/2000/3000/4000/10000 in both modes, ~15-18 min)
//
// For each run: fire load_test.js -> wait for LightingControlQueue to drain ->
// wait for logs to land -> latency_report.js. Everything ends up in results/.

const { spawnSync } = require('child_process');
const { SQSClient, GetQueueAttributesCommand } = require('@aws-sdk/client-sqs');
const fs = require('fs');

const QUEUE_URL = 'https://sqs.ap-southeast-2.amazonaws.com/669394141948/LightingControlQueue';
const sqs = new SQSClient({ region: 'ap-southeast-2' });
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const QUICK = process.argv.includes('--quick');
const KNEE = process.argv.includes('--knee');

// [label, events, mode, batch, durationS]
const MATRIX = QUICK ? [
  ['quick-raw-5000', 5000, 'raw', 1, 0],
  ['quick-batched-5000', 5000, 'batched', 25, 0]
] : KNEE ? [
  // E5: find where per-message degradation starts (Distinction jumped 1000 -> 5000),
  // and how far edge batching pushes that point out
  ...[1500, 2000, 3000, 4000, 10000].flatMap(n => [
    [`E5-raw-${n}`, n, 'raw', 1, 0],
    [`E5-batched-${n}`, n, 'batched', 25, 0]
  ])
] : [
  // warm-up (discarded in the report)
  ['warmup', 500, 'raw', 1, 0],

  // E1: burst sweep, raw vs edge-batched (maxBatch 25)
  ...[50, 100, 200, 500, 1000, 5000].flatMap(n => [
    [`E1-raw-${n}`, n, 'raw', 1, 0],
    [`E1-batched-${n}`, n, 'batched', 25, 0]
  ]),

  // E2: repeat the critical 5000 burst two more times each (3 total with E1) for variance
  ['E2-raw-5000-r2', 5000, 'raw', 1, 0],
  ['E2-batched-5000-r2', 5000, 'batched', 25, 0],
  ['E2-raw-5000-r3', 5000, 'raw', 1, 0],
  ['E2-batched-5000-r3', 5000, 'batched', 25, 0],

  // E3: envelope-size sensitivity at the 5000 burst
  ['E3-batched-5000-b10', 5000, 'batched', 10, 0],
  ['E3-batched-5000-b50', 5000, 'batched', 50, 0],
  ['E3-batched-5000-b100', 5000, 'batched', 100, 0],

  // E4: sustained load (5000 events spread over 30s) — shows the edge-wait trade-off
  ['E4-raw-5000-30s', 5000, 'raw', 1, 30],
  ['E4-batched-5000-30s', 5000, 'batched', 25, 30]
];

async function queueDepth() {
  const r = await sqs.send(new GetQueueAttributesCommand({
    QueueUrl: QUEUE_URL,
    AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
  }));
  return parseInt(r.Attributes.ApproximateNumberOfMessages) + parseInt(r.Attributes.ApproximateNumberOfMessagesNotVisible);
}

async function waitForDrain() {
  let zeroCount = 0;
  const start = Date.now();
  while (zeroCount < 3 && Date.now() - start < 5 * 60 * 1000) {
    await sleep(2000);
    zeroCount = (await queueDepth()) === 0 ? zeroCount + 1 : 0;
  }
}

function readLastLatency(runId) {
  if (!fs.existsSync('results/latency.jsonl')) return null;
  const lines = fs.readFileSync('results/latency.jsonl', 'utf8').trim().split('\n').map(l => JSON.parse(l));
  return lines.filter(l => l.runId === runId).pop() || null;
}

(async () => {
  console.log(`Running ${MATRIX.length} experiment runs${QUICK ? ' (quick mode)' : KNEE ? ' (knee-finding mode)' : ''}...\n`);
  const stamp = Date.now().toString(36);

  for (const [label, n, mode, batch, dur] of MATRIX) {
    const runId = `${label}-${stamp}`;
    console.log(`\n=== ${label}: ${n} events, ${mode}${mode === 'batched' ? ` b=${batch}` : ''}, ${dur ? `${dur}s sustained` : 'burst'} ===`);

    await waitForDrain(); // make sure the previous run is fully cleared
    spawnSync('node', ['load_test.js', String(n), '--mode', mode, '--batch', String(batch), '--duration', String(dur), '--run', runId], { stdio: 'inherit' });

    process.stdout.write('Waiting for queue to drain...');
    await waitForDrain();
    console.log(' done. Waiting 30s for logs to land...');
    await sleep(30000);

    for (let attempt = 0; attempt < 4; attempt++) {
      spawnSync('node', ['latency_report.js', runId, '60'], { stdio: 'inherit' });
      const r = readLastLatency(runId);
      if (r && r.events >= n) break;
      console.log(`Only ${r ? r.events : 0}/${n} events logged so far, retrying in 20s...`);
      await sleep(20000);
    }
  }

  console.log('\nAll runs complete. Send back: results/runs.jsonl and results/latency.jsonl');
})().catch(e => { console.error(e); process.exit(1); });
