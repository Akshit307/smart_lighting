const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');

const REGION = 'ap-southeast-2';
const ddbClient = new DynamoDBClient({ region: REGION });
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

exports.handler = async (event) => {
  console.log(`[Scheduling Lambda] Received ${event.Records.length} message(s)`);

  const results = await Promise.allSettled(
    event.Records.map(record => processRecord(record))
  );

  const batchItemFailures = [];
  results.forEach((result, i) => {
    if (result.status === 'rejected') {
      batchItemFailures.push({ itemIdentifier: event.Records[i].messageId });
    }
  });

  if (batchItemFailures.length > 0) {
    console.error(`[Scheduling Lambda] ${batchItemFailures.length} message(s) failed`);
  }

  return { batchItemFailures };
};

// Accepts a raw device event or an edge batch envelope { type: 'batch', events: [...] }
function extractEvents(body) {
  const parsed = JSON.parse(body);
  if (parsed && parsed.type === 'batch' && Array.isArray(parsed.events)) return parsed.events;
  return [parsed];
}

async function processRecord(record) {
  // events inside one envelope are processed in order (they may refer to the same room)
  for (const payload of extractEvents(record.body)) {
    await processEvent(payload);
  }
}

async function processEvent(payload) {

  if (payload.overrideState === undefined) {
    return;
  }

  console.log('[Scheduling Lambda] Processing manual override:', payload);

  const deviceId = `switch_${payload.propertyId}_${payload.roomId}`;
  const lightDeviceId = `light_${payload.propertyId}_${payload.roomId}`;

  await ddbDocClient.send(new PutCommand({
    TableName: 'DeviceState',
    Item: {
      deviceId: deviceId,
      propertyId: payload.propertyId,
      roomId: payload.roomId,
      state: payload.overrideState,
      lastUpdated: new Date().toISOString(),
      overrideActive: true
    }
  }));

  await ddbDocClient.send(new PutCommand({
    TableName: 'DeviceState',
    Item: {
      deviceId: lightDeviceId,
      propertyId: payload.propertyId,
      roomId: payload.roomId,
      state: payload.overrideState,
      lastUpdated: new Date().toISOString(),
      overrideActive: true
    }
  }));

  console.log(`[Scheduling Lambda] Override applied: ${lightDeviceId} -> ${payload.overrideState} (overrideActive: true)`);
}