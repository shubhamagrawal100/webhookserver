// kafkaProducer.js

require('dotenv').config();
const crypto = require('crypto');
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'webhook-server',
  brokers: process.env.KAFKA_BROKERS.split(','),
  ssl: true,
  sasl: {
    mechanism: 'plain', // scram-sha-256 or scram-sha-512 can also be used
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET
  }
});

const producer = kafka.producer();
const createHash = async (senderPhone, receiverPhone) => {
  // Sort data
  const data = [senderPhone, receiverPhone].sort();

  // Concatenate
  const combinedString = data.join('');

  // Hash using SHA-256
  const hash = crypto.createHash('sha256').update(combinedString).digest('hex');
  return hash;
};

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
            numPartitions: 3 // Number of partitions
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

const sendMessage = async (message) => {
  const receiverPhone = message.display_number;
  const senderPhone = message.from;
  const partitionKey = await createHash(senderPhone, receiverPhone); 
  const topic = `notification-message`;
  await createTopic(topic);

  await producer.send({
    topic,
    messages: [
      { key: receiverPhone, value: JSON.stringify(message) },
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
