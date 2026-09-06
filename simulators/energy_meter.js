const mqtt = require('mqtt');

const client = mqtt.connect('mqtt://localhost:1883');

const propertyId = 'property_001';
const roomId = 'living_room';
const topic = `smarthome/${propertyId}/${roomId}/energy`;

client.on('connect', () => {
  console.log(`Energy meter connected. Publishing to ${topic}`);

  setInterval(() => {
    // simulate wattage draw - low baseline, higher when lights are likely on
    const hour = new Date().getHours();
    const likelyOn = hour < 7 || hour > 18;
    const baseWatts = likelyOn ? 40 : 5;
    const watts = Math.round(baseWatts + Math.random() * 10);

    const payload = {
      deviceId: `energy_${propertyId}_${roomId}`,
      propertyId,
      roomId,
      watts,
      time: Date.now()
    };

    client.publish(topic, JSON.stringify(payload));
    console.log('Published:', payload);
  }, 3000);
});

client.on('error', (err) => {
  console.error('MQTT error:', err);
});