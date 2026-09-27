const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');

const REGION = 'ap-southeast-2';
const ddbClient = new DynamoDBClient({ region: REGION });
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

exports.handler = async (event) => {
  console.log(`[Lighting Control Lambda] Received ${event.Records.length} message(s)`);

  const results = await Promise.allSettled(
    event.Records.map(record => processRecord(record))
  );

  const failures = results.filter(r => r.status === 'rejected');
  if (failures.length > 0) {
    console.error(`[Lighting Control Lambda] ${failures.length} message(s) failed`);
    // Throwing here tells Lambda/SQS to retry the failed batch
    throw new Error(`${failures.length} messages failed processing`);
  }

  return { statusCode: 200, processed: event.Records.length };
};

async function processRecord(record) {
  const payload = JSON.parse(record.body);

  console.log('[Lighting Control Lambda] Processing:', payload);

  const deviceId = payload.deviceId;
  const state = payload.motion === true ? 'on' : 'off';

  await ddbDocClient.send(new PutCommand({
    TableName: 'DeviceState',
    Item: {
      deviceId: deviceId,
      propertyId: payload.propertyId,
      roomId: payload.roomId,
      state: state,
      lastUpdated: new Date().toISOString(),
      overrideActive: false
    }
  }));

  console.log(`[Lighting Control Lambda] Actuated: ${deviceId} -> ${state}`);
}