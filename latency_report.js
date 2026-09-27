// latency_report.js
// Pulls latency + invocation stats for one load-test run straight from CloudWatch Logs Insights.
//
// Usage:  node latency_report.js <runId> [minutesBack=30]
// Output: printed table + appended to results/latency.jsonl (send that file back for the report)

const {
  CloudWatchLogsClient, StartQueryCommand, GetQueryResultsCommand
} = require('@aws-sdk/client-cloudwatch-logs');
const fs = require('fs');

const LOG_GROUP = '/aws/lambda/LightingControlFunction';
const client = new CloudWatchLogsClient({ region: 'ap-southeast-2' });

const RUN_ID = process.argv[2];
const MINUTES_BACK = parseInt(process.argv[3]) || 30;
if (!RUN_ID) {
  console.error('Usage: node latency_report.js <runId> [minutesBack]');
  process.exit(1);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function query(queryString) {
  const endTime = Math.floor(Date.now() / 1000);
  const startTime = endTime - MINUTES_BACK * 60;
  const { queryId } = await client.send(new StartQueryCommand({
    logGroupName: LOG_GROUP, startTime, endTime, queryString
  }));
  for (;;) {
    await sleep(1000);
    const res = await client.send(new GetQueryResultsCommand({ queryId }));
    if (['Complete', 'Failed', 'Cancelled', 'Timeout'].includes(res.status)) {
      if (res.status !== 'Complete') throw new Error(`Query ${res.status}`);
      return (res.results || []).map(row => Object.fromEntries(row.map(f => [f.field, f.value])));
    }
  }
}

(async () => {
  const latencyQ = `
    fields @message
    | filter @message like /\\[LATENCY\\] runId=${RUN_ID} /
    | parse @message "edgeWaitMs=* latencyMs=*" as edgeWaitMs, latencyMs
    | stats count(*) as events,
            avg(latencyMs) as avgMs,
            pct(latencyMs, 50) as p50,
            pct(latencyMs, 95) as p95,
            pct(latencyMs, 99) as p99,
            max(latencyMs) as maxMs,
            avg(edgeWaitMs) as avgEdgeWaitMs,
            min(@timestamp) as firstDone,
            max(@timestamp) as lastDone`;

  const invocationQ = `
    fields @message
    | filter @message like /\\[INVOCATION\\] runId=${RUN_ID} /
    | parse @message "records=* events=* failed=* durationMs=*" as records, evts, failed, durMs
    | stats count(*) as invocations, sum(records) as sqsRecords, sum(failed) as failedRecords, avg(durMs) as avgInvocationMs`;

  const errorQ = `
    fields @message
    | filter @message like /message\\(s\\) failed/
    | stats count(*) as errors`;

  console.log(`Querying CloudWatch Logs Insights for run ${RUN_ID}...`);
  const [lat, inv, err] = await Promise.all([query(latencyQ), query(invocationQ), query(errorQ)]);
  const l = lat[0] || {};
  const v = inv[0] || {};
  const n = (x) => (x === undefined ? null : Math.round(parseFloat(x)));

  const result = {
    runId: RUN_ID,
    events: n(l.events),
    p50: n(l.p50), p95: n(l.p95), p99: n(l.p99), max: n(l.maxMs), avg: n(l.avgMs),
    avgEdgeWaitMs: n(l.avgEdgeWaitMs),
    lambdaInvocations: n(v.invocations),
    sqsRecordsProcessed: n(v.sqsRecords),
    failedRecords: n(v.failedRecords),
    avgInvocationMs: n(v.avgInvocationMs),
    errorsInWindow: n((err[0] || {}).errors) || 0,
    queriedAt: new Date().toISOString()
  };

  console.table(result);
  if (!result.events) console.log('No [LATENCY] lines found yet — wait a bit longer or check the runId.');

  fs.mkdirSync('results', { recursive: true });
  fs.appendFileSync('results/latency.jsonl', JSON.stringify(result) + '\n');
  console.log('Saved to results/latency.jsonl');
})().catch(e => { console.error(e); process.exit(1); });
