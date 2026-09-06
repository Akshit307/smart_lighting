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

  const failures = results.filter(r => r.status === 'rejected');
  if (failures.length > 0) {
    console.error(`[Notification Lambda] ${failures.length} message(s) failed`);
    throw new Error(`${failures.length} messages failed processing`);
  }

  return { statusCode: 200, processed: event.Records.length };
};

async function processRecord(record) {
  const snsEnvelope = JSON.parse(record.body);
  const payload = JSON.parse(snsEnvelope.Message);

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

  // motion === false: check how long the room has reportedly been empty
  // AND whether the light is currently on, for that room
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
    return; // light isn't on, no anomaly possible
  }

  if (!motionState.Item) {
    return; // no occupancy history yet
  }

  const lastOccupied = new Date(motionState.Item.lastUpdated).getTime();
  const now = Date.now();

  if (now - lastOccupied > ANOMALY_THRESHOLD_MS) {
    console.warn(`[Notification Lambda] ANOMALY: Light ON in ${payload.roomId} (${payload.propertyId}) with no occupancy for over 2 hours!`);
    // In a full deployment this would publish to a separate "alerts" SNS topic
    // for email/SMS delivery. For this project, the CloudWatch log entry itself
    // serves as the alert evidence.
  }
}