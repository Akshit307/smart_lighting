// LightingControlFunction — HD version
// Accepts BOTH message formats on LightingControlQueue:
//   - raw device event   { deviceId, propertyId, roomId, motion, time, ... }
//   - edge batch envelope { type: 'batch', gatewayId, batchedAt, events: [ ... ] }
// State is written with DynamoDB BatchWriteItem (25 items per call) instead of one PutItem per event.
// Partial batch failure reporting: only failed SQS records are returned for retry.

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const REGION = 'ap-southeast-2';
const TABLE = 'DeviceState';
const ddbClient = new DynamoDBClient({ region: REGION });
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

const DDB_BATCH_LIMIT = 25;
const MAX_RETRIES = 5;
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

exports.handler = async (event) => {
  const invokedAt = Date.now();
  const batchItemFailures = [];
  let totalEvents = 0;
  const runIds = new Set();

  const results = await Promise.allSettled(event.Records.map(record => processRecord(record)));

  results.forEach((result, i) => {
    if (result.status === 'rejected') {
      console.error(`[Lighting Control Lambda] message(s) failed: ${event.Records[i].messageId}: ${result.reason && result.reason.message}`);
      batchItemFailures.push({ itemIdentifier: event.Records[i].messageId });
    } else {
      totalEvents += result.value.count;
      if (result.value.runId) runIds.add(result.value.runId);
    }
  });

  // One summary line per invocation — used to count invocations per run
  console.log(`[INVOCATION] runId=${[...runIds].join(',') || 'none'} records=${event.Records.length} events=${totalEvents} failed=${batchItemFailures.length} durationMs=${Date.now() - invokedAt}`);

  return { batchItemFailures };
};

// Returns the list of device events carried by one SQS record
function extractEvents(body) {
  const parsed = JSON.parse(body);
  if (parsed && parsed.type === 'batch' && Array.isArray(parsed.events)) {
    return { events: parsed.events, batchedAt: parsed.batchedAt, batched: true };
  }
  return { events: [parsed], batchedAt: parsed.time, batched: false };
}

async function processRecord(record) {
  const receivedAt = Date.now();
  const { events, batchedAt, batched } = extractEvents(record.body);
  if (events.length === 0) return { count: 0 };

  // DynamoDB rejects a BatchWriteItem containing the same key twice, so keep only the
  // newest event per device (for a current-state table the older one would be overwritten anyway)
  const latestByDevice = new Map();
  for (const evt of events) {
    const prev = latestByDevice.get(evt.deviceId);
    if (!prev || (evt.time || 0) >= (prev.time || 0)) latestByDevice.set(evt.deviceId, evt);
  }

  const nowIso = new Date().toISOString();
  const items = [...latestByDevice.values()].map(evt => ({
    deviceId: evt.deviceId,
    propertyId: evt.propertyId,
    roomId: evt.roomId,
    state: evt.motion === true ? 'on' : 'off',
    lastUpdated: nowIso,
    overrideActive: false
  }));

  const chunks = [];
  for (let i = 0; i < items.length; i += DDB_BATCH_LIMIT) chunks.push(items.slice(i, i + DDB_BATCH_LIMIT));
  await Promise.all(chunks.map(writeChunk));

  const completedAt = Date.now();
  for (const evt of events) {
    const edgeWaitMs = batched ? batchedAt - evt.time : 0;
    console.log(`[LATENCY] runId=${evt.runId || 'live'} mode=${batched ? 'batched' : 'raw'} deviceId=${evt.deviceId} publishedAt=${evt.time} batchedAt=${batchedAt} receivedAt=${receivedAt} completedAt=${completedAt} edgeWaitMs=${edgeWaitMs} latencyMs=${completedAt - evt.time}`);
  }

  return { count: events.length, runId: events[0].runId };
}

// BatchWriteItem with retry of UnprocessedItems (exponential backoff)
async function writeChunk(items) {
  let requestItems = { [TABLE]: items.map(Item => ({ PutRequest: { Item } })) };
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const res = await ddbDocClient.send(new BatchWriteCommand({ RequestItems: requestItems }));
    const unprocessed = res.UnprocessedItems && res.UnprocessedItems[TABLE];
    if (!unprocessed || unprocessed.length === 0) return;
    requestItems = { [TABLE]: unprocessed };
    await sleep(Math.min(50 * 2 ** attempt, 1000));
  }
  throw new Error(`BatchWrite still had unprocessed items after ${MAX_RETRIES} retries`);
}
