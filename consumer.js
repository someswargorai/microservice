const express = require("express");
const { Kafka } = require("kafkajs");
const nodemailer = require("nodemailer");
const client = require("prom-client");
require("dotenv").config();

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "email-worker",
  brokers: ["localhost:9092"],
});

const emailsSent = new client.Counter({
  name: "emails_sent_total",
  help: "Total emails successfully sent",
});

const emailsFailed = new client.Counter({
  name: "emails_failed_total",
  help: "Total emails failed to send",
});

const timesTakenToSendEmails = new client.Histogram({
  name: "times_taken_to_send_emails",
  help: "Total time taken to send all emails",
  buckets: [0.5, 1, 2, 3, 4, 5, 6, 7, 8],
});

const collectDefaultMetrics = client.collectDefaultMetrics;

collectDefaultMetrics({ register: client.register });

app.get("/metrics", async (req, res) => {
  res.set("Content-Type", client.register.contentType);
  res.end(await client.register.metrics());
});

const consumer = kafka.consumer({ groupId: "email-workers" });

const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
});

async function sendEmail(email, name) {
  try {
    console.log(`Sending email to ${name}`);
    const end = timesTakenToSendEmails.startTimer();
    const info = await transporter.sendMail({
      from: process.env.SMTP_USER,
      to: email,
      subject: "Test Email from Node",
      html: `<h2>Hello ${name}</h2><p>This email was sent using Nodemailer.</p>`,
    });

    emailsSent.inc();
    end();
    console.log(`Email sent to ${email}`);
    console.log("Message ID:", info.messageId);
  } catch (error) {
    emailsFailed.inc();
    console.error("Email failed:", error);
  }
}

async function start() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "email-jobs",
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        await sendEmail(data.email, data.name);
      } catch (err) {
        console.error("Failed to process message:", err);
      }
    },
  });

  app.listen(3001, () => {
    console.log("Consumer running on port 3001");
  });
}

start();
