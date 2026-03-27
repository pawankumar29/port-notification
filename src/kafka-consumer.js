import { Kafka } from "kafkajs";
import logger from "./logger.js";
import { sendEmail } from "./services/email.js";
import { sendWhatsApp } from "./services/whatsapp.js";

export async function startKafkaConsumer(config) {
  const { brokers, clientId, groupId, topic, fromBeginning } = config.kafka;

  const kafka = new Kafka({
    clientId,
    brokers,
  });

  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  });

  await consumer.connect();
  logger.info(
    `Kafka consumer connected (clientId=${clientId}, groupId=${groupId})`
  );

  await consumer.subscribe({ topic, fromBeginning });
  logger.info(
    `Subscribed to topic: ${topic} (fromBeginning=${fromBeginning})`
  );

  await consumer.run({
    eachMessage: async ({ topic: t, partition, message }) => {
      const offset = message.offset;
      const key = message.key?.toString();
      const value = message.value?.toString() ?? "";

      logger.info(
        `[${t}][p=${partition}][offset=${offset}] key=${key ?? "(none)"}`
      );
      logger.debug(`Raw payload: ${value.slice(0, 500)}${value.length > 500 ? "…" : ""}`);

// console.log('data:::::::',JSON.stringify(value));
console.log('data:::::::',value);


      const parsed = JSON.parse(value);

      try {

        logger.info(parsed);
                logger.info(JSON.stringify(parsed));


        await sendEmail(config.email, {
  to: 'pk2027317@gmail.com',
  subject: `New Contact Request from ${parsed.name}`,
  text: `
New Contact Request

Name: ${parsed.name}
Email: ${parsed.email}
Budget: ${parsed.budget}
Message: ${parsed.message}
  `,
  html: `
    <h2>New Contact Request</h2>
    <p><b>Name:</b> ${parsed.name}</p>
    <p><b>Email:</b> ${parsed.email}</p>
    <p><b>Budget:</b> ${parsed.budget}</p>
    <p><b>Message:</b> ${parsed.message}</p>
  `
});


  sendWhatsApp(config.whatsapp, {
            to: parsed.whatsappTo,
            body: parsed.message || parsed.subject,
          }).catch((err) => {
            logger.error(`WhatsApp failed: ${err.message}`);
          })
      } catch (err) {
        logger.error(`Notification handling error: ${err.message}`);
      }
    },
  });

  return consumer;
}
