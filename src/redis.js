import "dotenv/config";
import Redis from "ioredis";
import logger from "./logger.js";

const redisUrl = process.env.REDIS_URL;
const redisHost = process.env.REDIS_HOST || "127.0.0.1";
const redisPort = Number(process.env.REDIS_PORT || 6390);

const redis = redisUrl
  ? new Redis(redisUrl, {
      retryStrategy: (times) => {
        if (times > 5) return null;
        return Math.min(times * 500, 3000);
      },
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
      connectTimeout: 10000,
    })
  : new Redis({
      host: redisHost,
      port: redisPort,
      retryStrategy: (times) => {
        if (times > 5) return null; // ← stop retrying after 5 attempts
        return Math.min(times * 500, 3000);
      },
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
    });

export const redisReady = new Promise((resolve, reject) => {
  redis.once("ready", () => {
    resolve();
  });

  // Only reject after retries are exhausted (retryStrategy returns null → emits "end")
  redis.once("end", () => {
    reject(
      new Error(
        `Cannot connect to Redis at ${redisUrl || `${redisHost}:${redisPort}`} after retries`,
      ),
    );
  });
});

redis.on("connect", () => logger.info("Redis: TCP connected"));
redis.on("ready", () => logger.info("Redis: ready"));
redis.on("close", () => logger.warn("Redis: connection closed"));
redis.on("reconnecting", (ms) => logger.warn(`Redis: reconnecting in ${ms}ms`));
redis.on("error", (err) => logger.error(`Redis error: ${err.message}`));

export default redis;
