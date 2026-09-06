const mqtt = require('mqtt');

const client = mqtt.connect('mqtt://localhost:1883');

const propertyId = 'property_001';
const roomId = 'living_room';
const topic = `smarthome/${propertyId}/${roomId}/motion`;

client.on('connect', () => {
  console.log(`Motion sensor connected. Publishing to ${topic}`);

  setInterval(() => {
    const motionDetected = Math.random() < 0.4; // 40% chance of motion per tick

    const payload = {
      deviceId: `motion_${propertyId}_${roomId}`,
      propertyId,
      roomId,
      motion: motionDetected,
      time: Date.now()
    };

    client.publish(topic, JSON.stringify(payload));
    console.log('Published:', payload);
  }, 3000); // every 3 seconds
});

client.on('error', (err) => {
  console.error('MQTT error:', err);
});