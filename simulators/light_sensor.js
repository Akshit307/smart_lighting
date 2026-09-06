const mqtt = require('mqtt');

const client = mqtt.connect('mqtt://localhost:1883');

const propertyId = 'property_001';
const roomId = 'living_room';
const topic = `smarthome/${propertyId}/${roomId}/light`;

client.on('connect', () => {
  console.log(`Light sensor connected. Publishing to ${topic}`);

  setInterval(() => {
    // simulate a day/night lux curve based on real time of day
    const hour = new Date().getHours() + new Date().getMinutes() / 60;
    let baseLux;

    if (hour >= 7 && hour <= 18) {
      // daytime: high lux, peaking at midday
      const distFromNoon = Math.abs(12 - hour);
      baseLux = 800 - (distFromNoon * 60);
    } else {
      // nighttime: low lux
      baseLux = 5;
    }

    const noise = (Math.random() - 0.5) * 40;
    const lux = Math.max(0, Math.round(baseLux + noise));

    const payload = {
      deviceId: `light_${propertyId}_${roomId}`,
      propertyId,
      roomId,
      lux,
      time: Date.now()
    };

    client.publish(topic, JSON.stringify(payload));
    console.log('Published:', payload);
  }, 3000);
});

client.on('error', (err) => {
  console.error('MQTT error:', err);
});