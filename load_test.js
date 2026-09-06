const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const snsClient = new SNSClient({ region: 'ap-southeast-2' });
const TOPIC_ARN = 'arn:aws:sns:ap-southeast-2:669394141948:SmartLightingEvents';

const NUM_MESSAGES = parseInt(process.argv[2]) || 50;

async function sendMessage(i) {
  const payload = {
    deviceId: `load_test_device_${i}`,
    propertyId: `property_${Math.floor(i / 10)}`,
    roomId: `room_${i % 10}`,
    lux: Math.random() < 0.5 ? 50 : 700,
    time: Date.now(),
    lightingCommand: Math.random() < 0.5 ? 'on' : 'off'
  };

  const command = new PublishCommand({
    TopicArn: TOPIC_ARN,
    Message: JSON.stringify(payload)
  });

  return snsClient.send(command);
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