  const express = require("express");
  const { Kafka } = require("kafkajs");
  const { createTopic, createTopics, deleteTopics } = require("./create-topic-partitions");
  const readline = require("readline");

  const kafka = new Kafka({
    clientId: "email-api",
    brokers: ["localhost:9092"],
  });

  const rl = readline.createInterface({
    input: process.stdin,
    output:process.stdout
  })

  const producer = kafka.producer();
  const nameProducer = kafka.producer();

  const app = express();
  app.use(express.json());

  async function start() {
    
    await producer.connect();
    await nameProducer.connect();
    await deleteTopics();
    await new Promise((res) => setTimeout(res, 3000));

    await createTopics([
      { topic: "email-jobs", partitions: 2, replicationFactor: 1 },
      { topic: "name-jobs", partitions: 4, replicationFactor: 1 },
    ]);

    app.post("/users", async (req, res) => {
      try {
        const users = req.body.users;

        const messages = users.map((user) => ({
          value: JSON.stringify(user),
        }));

      
        await producer.send({
          topic: "email-jobs",
          messages,
        });

        res.json({ message: "Jobs pushed to Kafka" });
      } catch (err) {
        console.error(err);
        res.status(500).json({ error: "Failed to push jobs" });
      }
    });

    app.post("/names", async (req, res) => {
      try {
        
        const { region, name } = req.body;

        await nameProducer.send({
          topic: "name-jobs",
          messages: [
            {
              key: region,
              value: JSON.stringify({ name }),
            },
          ],
        });

        res.json({ message: "Name job pushed to Kafka" });
      } catch (err) {
        console.error(err);
        res.status(500).json({ error: "Failed to push name job" });
      }
    });

    app.listen(3000, () => {
      console.log("API running on port 3000");
    });
  }

  start();
