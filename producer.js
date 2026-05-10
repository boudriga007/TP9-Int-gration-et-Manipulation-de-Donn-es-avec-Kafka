const { Kafka } = require('kafkajs');
require('dotenv').config();

const kafka = new Kafka({
  clientId: 'tp9-producer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const producer = kafka.producer();
const topic = process.env.KAFKA_TOPIC || 'test-topic';

const run = async () => {
  await producer.connect();
  console.log('Producteur connecté');

  setInterval(async () => {
    const event = {
      deviceId: 'sensor-01',
      temperature: Number((20 + Math.random() * 10).toFixed(2)),
      createdAt: new Date().toISOString(),
    };
    await producer.send({
      topic,
      messages: [{ key: event.deviceId, value: JSON.stringify(event) }],
    });
    console.log('Message produit:', event);
  }, 1000);
};

run().catch(console.error);