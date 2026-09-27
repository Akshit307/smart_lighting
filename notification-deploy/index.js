const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');

const REGION = 'ap-southeast-2';
const ddbClient = new DynamoDBClient({ region: REGION });
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

const ANOMALY_THRESHOLD_MS = 2 * 60 * 60 * 1000; // 2 hours

exports.handler = async (event) => {
  console.log(`[Notification Lambda] Received ${event.Records.length} message(s)`);

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
    console.error(`[Notification Lambda] ${batchItemFailures.length} message(s) failed`);
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

  // Only care about motion events for occupancy tracking
  if (payload.motion === undefined) {
    return; // not a motion event, nothing to check
  }

  const deviceKey = `motion_${payload.propertyId}_${payload.roomId}`;

  if (payload.motion === true) {
    // occupancy detected - reset the last-seen timestamp
    await ddbDocClient.send(new PutCommand({
      TableName: 'DeviceState',
      Item: {
        deviceId: deviceKey,
        propertyId: payload.propertyId,
        roomId: payload.roomId,
        state: 'occupied',
        lastUpdated: new Date().toISOString(),
        overrideActive: false
      }
    }));
    console.log(`[Notification Lambda] Occupancy refreshed for ${deviceKey}`);
    return;
  }

  const lightDeviceKey = `light_${payload.propertyId}_${payload.roomId}`;

  const lightState = await ddbDocClient.send(new GetCommand({
    TableName: 'DeviceState',
    Key: { deviceId: lightDeviceKey }
  }));

  const motionState = await ddbDocClient.send(new GetCommand({
    TableName: 'DeviceState',
    Key: { deviceId: deviceKey }
  }));

  if (!lightState.Item || lightState.Item.state !== 'on') {
    return;
  }

  if (!motionState.Item) {
    return;
  }

  const lastOccupied = new Date(motionState.Item.lastUpdated).getTime();
  const now = Date.now();

  if (now - lastOccupied > ANOMALY_THRESHOLD_MS) {
    console.warn(`[Notification Lambda] ANOMALY: Light ON in ${payload.roomId} (${payload.propertyId}) with no occupancy for over 2 hours!`);
  }
}