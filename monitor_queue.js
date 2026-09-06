const { SQSClient, GetQueueAttributesCommand } = require('@aws-sdk/client-sqs');

const sqsClient = new SQSClient({ region: 'ap-southeast-2' });
const QUEUE_URL = 'https://sqs.ap-southeast-2.amazonaws.com/669394141948/LightingControlQueue';

const DURATION_SECONDS = parseInt(process.argv[2]) || 30;

async function sample() {
  const result = await sqsClient.send(new GetQueueAttributesCommand({
    QueueUrl: QUEUE_URL,
    AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
  }));
  const visible = result.Attributes.ApproximateNumberOfMessages;
  const inFlight = result.Attributes.ApproximateNumberOfMessagesNotVisible;
  console.log(`${new Date().toISOString()} | Waiting: ${visible} | In-flight: ${inFlight}`);
}

async function run() {
  console.log(`Monitoring queue depth for ${DURATION_SECONDS} seconds...`);
  const endTime = Date.now() + DURATION_SECONDS * 1000;
  while (Date.now() < endTime) {
    await sample();
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  console.log('Monitoring complete.');
}

run();