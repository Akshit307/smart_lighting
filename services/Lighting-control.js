const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');

const REGION = 'ap-southeast-2';
const QUEUE_URL = 'https://sqs.ap-southeast-2.amazonaws.com/669394141948/LightingControlQueue';

const sqsClient = new SQSClient({ region: REGION });
const ddbClient = new DynamoDBClient({ region: REGION });
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

console.log('[Lighting Control] Service starting, polling SQS...');

async function pollQueue() {
  while (true) {
    try {
      const receiveResult = await sqsClient.send(new ReceiveMessageCommand({
        QueueUrl: QUEUE_URL,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 10 // long polling
      }));

      const messages = receiveResult.Messages || [];

      for (const message of messages) {
        await processMessage(message);

        // delete from queue once processed
        await sqsClient.send(new DeleteMessageCommand({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: message.ReceiptHandle
        }));
      }
    } catch (err) {
      console.error('[Lighting Control] Poll error:', err);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
}

async function processMessage(message) {
  try {
    const snsEnvelope = JSON.parse(message.Body);
    const payload = JSON.parse(snsEnvelope.Message);

    console.log('[Lighting Control] Processing:', payload);

    const deviceId = payload.deviceId;
    const state = payload.lightingCommand === 'on' ? 'on' : 'off';

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

    console.log(`[Lighting Control] Actuated: ${deviceId} -> ${state} (written to DynamoDB)`);
  } catch (err) {
    console.error('[Lighting Control] Failed to process message:', err);
  }
}

pollQueue();