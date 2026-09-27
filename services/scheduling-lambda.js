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

  const failures = results.filter(r => r.status === 'rejected');
  if (failures.length > 0) {
    console.error(`[Scheduling Lambda] ${failures.length} message(s) failed`);
    throw new Error(`${failures.length} messages failed processing`);
  }

  return { statusCode: 200, processed: event.Records.length };
};

async function processRecord(record) {
  const snsEnvelope = JSON.parse(record.body);
  const payload = JSON.parse(snsEnvelope.Message);

  // Only care about manual override events from the smart switch
  if (payload.overrideState === undefined) {
    return; // not a switch override event, nothing to schedule
  }

  console.log('[Scheduling Lambda] Processing manual override:', payload);

  const deviceId = `switch_${payload.propertyId}_${payload.roomId}`;
  const lightDeviceId = `light_${payload.propertyId}_${payload.roomId}`;

  // record the override itself
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

  // manual override takes priority - force the light's state to match, with override flag set
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