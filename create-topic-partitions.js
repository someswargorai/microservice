// create-topics.js
const { Kafka } = require("kafkajs");

async function createTopics(topicsConfig) {
  const kafka = new Kafka({
    clientId: "admin-client",
    brokers: ["localhost:9092"],
  });
  const admin = kafka.admin();
  await admin.connect();

  try {
    const existingTopics = await admin.listTopics();

    console.log(existingTopics);
    
    for (const { topic, partitions, replicationFactor } of topicsConfig) {
      if (!existingTopics.includes(topic)) {
        const created = await admin.createTopics({
          topics: [{ topic, numPartitions: partitions, replicationFactor }],
          waitForLeaders: true,
        });
        console.log(`Topic ${topic} created:`, created);
      } else {
        console.log(`Topic ${topic} already exists`);
      }
    }
  } catch (err) {
    console.error("Topic creation failed:", err);
  } finally {
    await admin.disconnect();
  }
}

async function deleteTopics() {
  const kafka = new Kafka({
    clientId: "admin-client",
    brokers: ["localhost:9092"],
  });
  const admin = kafka.admin();
  await admin.connect();

  try {
    await admin.deleteTopics({
      topics: ["email-jobs", "name-jobs"],
    });
    console.log("Topics deleted successfully");
  } catch (err) {
    console.error("Failed to delete topics:", err);
  } finally {
    await admin.disconnect();
  }
}

deleteTopics();

module.exports = { createTopics, deleteTopics };
