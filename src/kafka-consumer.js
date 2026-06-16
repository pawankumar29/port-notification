import { Kafka } from "kafkajs";
import logger from "./logger.js";
import { sendEmail } from "./services/email.js";
import { sendWhatsApp } from "./services/whatsapp.js";

export async function startKafkaConsumer(config) {
  const {
    brokers,
    clientId,
    groupId,
    topic: topics,
    fromBeginning,
    username,
    password,
  } = config.kafka;

  // const kafka = new Kafka({
  //   clientId,
  //   brokers,
  // });

  const kafka = new Kafka({
    clientId,
    brokers,
    ssl: {
      rejectUnauthorized: false, // ← bas yeh
    },
    sasl: {
      mechanism: "plain",
      username,
      password,
    },
    connectionTimeout: 10000,
    authenticationTimeout: 10000,
  });

  console.log({
    brokers,
    username,
    passwordExists: !!password,
  });

  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  });

  await consumer.connect();
  logger.info(
    `Kafka consumer connected (clientId=${clientId}, groupId=${groupId})`,
  );

  await consumer.subscribe({ topics, fromBeginning });
  logger.info(
    `Subscribed to topics: ${topics.join(", ")} (fromBeginning=${fromBeginning})`,
  );

  await consumer.run({
    eachMessage: async ({ topic: t, partition, message }) => {
      const offset = message.offset;
      const key = message.key?.toString();
      const value = message.value?.toString() ?? "";

      logger.info(
        `Kafka message received from [${t}]: key=${key ?? "(none)"}, offset=${offset}`,
      );

      try {
        const parsed = JSON.parse(value);
        logger.info(`Parsed Payload: ${JSON.stringify(parsed)}`);

        let emailSubject = `New Contact Request from ${parsed.name}`;
        // let emailBody = `Name: ${parsed.name}\nEmail: ${parsed.email}\nMessage: ${parsed.message}`;
        let emailBody = `Name: ${parsed.name}\nEmail: ${parsed.email}\nBudget: ${parsed.budget || "Not specified"}\nMessage: ${parsed.message}`;


        if (parsed.event === "payment_success") {
          emailSubject = `💰 Payment Successful: ${parsed.paymentId}`;
          emailBody = `Your payment of ${parsed.amount} was successful!\nPayment ID: ${parsed.paymentId}\nCustomer: ${parsed.customerEmail}`;
        } else if (parsed.event === "payment_failed") {
          emailSubject = `❌ Payment Failed: ${parsed.paymentId}`;
          emailBody = `Your payment of ${parsed.amount} has failed.\nPayment ID: ${parsed.paymentId}\nCustomer: ${parsed.customerEmail}`;
        } else if (parsed.event === "payment_pending") {
          emailSubject = `⏳ Payment Pending: ${parsed.paymentId}`;
          emailBody = `Your payment of ${parsed.amount} is currently pending.\nPayment ID: ${parsed.paymentId}\nCustomer: ${parsed.customerEmail}`;
        }

        await sendEmail(config.email, {
          to: parsed.customerEmail || "pk2027317@gmail.com",
          subject: emailSubject,
          text: emailBody,
          html: `<h2>Notification</h2><p>${emailBody.replace(/\n/g, "<br>")}</p>`,
        });

        if (parsed.whatsappTo) {
          await sendWhatsApp(config.whatsapp, {
            to: parsed.whatsappTo,
            body: emailBody,
          });
        }
      } catch (err) {
        logger.error(`Notification handling error: ${err.message}`);
      }
    },
  });

  return consumer;
}
