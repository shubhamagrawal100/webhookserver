// kafkaProducer.js

require('dotenv').config();
const { Kafka } = require('kafkajs');

// Kafka configuration
const kafka = new Kafka({
  clientId: 'webhook-server',
  brokers: process.env.KAFKA_BROKERS.split(','), // Set this environment variable with broker addresses
});

const producer = kafka.producer();

const createTopic = async (topic) => {
  const admin = kafka.admin();
  await admin.connect();

  try {
    // Check if the topic already exists
    const topics = await admin.listTopics();
    if (!topics.includes(topic)) {
      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: 3, // Number of partitions
            replicationFactor: 2, // Replication factor
          },
        ],
      });
      console.log(`Topic '${topic}' created with 3 partitions and replication factor 2.`);
    }
  } catch (error) {
    console.error('Error creating topic:', error);
  } finally {
    await admin.disconnect();
  }
};

const sendMessage = async (notificationType, message) => {
  const topic = `notifications-${notificationType}`;
  
  await createTopic(topic);

  await producer.send({
    topic,
    messages: [
      { value: JSON.stringify(message) },
    ],
  });

  console.log(`Message sent to topic '${topic}':`, message);
};

const initProducer = async () => {
  await producer.connect();
  console.log('Kafka producer connected.');
};

const closeProducer = async () => {
  await producer.disconnect();
  console.log('Kafka producer disconnected.');
};

module.exports = {
  initProducer,
  closeProducer,
  sendMessage,
};
