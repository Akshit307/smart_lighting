const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

const sqsClient = new SQSClient({
  region: 'ap-southeast-2',
  requestHandler: new NodeHttpHandler({
    connectionTimeout: 5000,
    socketTimeout: 5000,
    maxSockets: 5000
  })
});

const QUEUE_URL = 'https://sqs.ap-southeast-2.amazonaws.com/669394141948/LightingControlQueue';

const NUM_MESSAGES = parseInt(process.argv[2]) || 50;

async function sendMessage(i) {
  const payload = {
    deviceId: `load_test_device_${i}`,
    propertyId: `property_${Math.floor(i / 10)}`,
    roomId: `room_${i % 10}`,
    motion: Math.random() < 0.5,
    time: Date.now()
  };

  const command = new SendMessageCommand({
    QueueUrl: QUEUE_URL,
    MessageBody: JSON.stringify(payload)
  });

  return sqsClient.send(command);
}

async function runLoadTest() {
  console.log(`Starting load test: publishing ${NUM_MESSAGES} messages as fast as possible...`);
  const startTime = Date.now();

  const promises = [];
  for (let i = 0; i < NUM_MESSAGES; i++) {
    promises.push(sendMessage(i));
  }

  await Promise.all(promises);

  const totalTime = Date.now() - startTime;
  console.log(`Done. Published ${NUM_MESSAGES} messages in ${totalTime}ms`);
  console.log(`Rate: ${(NUM_MESSAGES / (totalTime / 1000)).toFixed(1)} messages/sec`);
}

runLoadTest();