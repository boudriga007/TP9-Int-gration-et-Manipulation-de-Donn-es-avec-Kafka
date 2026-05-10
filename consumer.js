
const { Kafka } = require('kafkajs');
const pool = require('./db');
require('dotenv').config();

const kafka = new Kafka({
  clientId: 'tp9-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'test-group' });
const topic = process.env.KAFKA_TOPIC || 'test-topic';

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  console.log('Consommateur connecté, en attente de messages...');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key    = message.key?.toString();
      const value  = message.value?.toString();
      const offset = message.offset;
      const payload = JSON.parse(value);

      console.log({ topic, partition, offset, key, value });

      await pool.query(
        `INSERT INTO kafka_messages (topic, partition, "offset", key, payload)
         VALUES ($1, $2, $3, $4, $5)`,
        [topic, partition, offset, key, payload]
      );
      console.log('Inséré en BDD ✓');
    },
  });
};

run().catch(console.error);