import { loadConfig } from "./config.js";
import logger from "./logger.js";
import { startKafkaConsumer } from "./kafka-consumer.js";

async function main() {
  const config = loadConfig();
  logger.info("Starting port-notification service…");
  logger.info(
    `Kafka: brokers=${config.kafka.brokers.join(",")} topic=${config.kafka.topic} groupId=${config.kafka.groupId}`
  );

  const consumer = await startKafkaConsumer(config);

  const shutdown = async (signal) => {
    logger.info(`Received ${signal}, disconnecting Kafka consumer…`);
    try {
      await consumer.disconnect();
    } catch (e) {
      logger.error(`Kafka disconnect error: ${e.message}`);
    }
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((err) => {
  logger.error(`Fatal: ${err.message}`);
  console.error(err);
  process.exit(1);
});
