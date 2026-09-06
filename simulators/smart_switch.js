const mqtt = require('mqtt');

const client = mqtt.connect('mqtt://localhost:1883');

const propertyId = 'property_001';
const roomId = 'living_room';
const topic = `smarthome/${propertyId}/${roomId}/switch`;

client.on('connect', () => {
  console.log(`Smart switch connected. Publishing to ${topic}`);

  setInterval(() => {
    // simulate occasional manual override presses (rare events)
    const overridePressed = Math.random() < 0.1; // 10% chance per tick

    if (overridePressed) {
      const state = Math.random() < 0.5 ? 'on' : 'off';

      const payload = {
        deviceId: `switch_${propertyId}_${roomId}`,
        propertyId,
        roomId,
        overrideState: state,
        time: Date.now()
      };

      client.publish(topic, JSON.stringify(payload));
      console.log('Published:', payload);
    }
  }, 5000); // check every 5 seconds
});

client.on('error', (err) => {
  console.error('MQTT error:', err);
});