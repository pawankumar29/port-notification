import "dotenv/config";
// import redis  from "./redis.js";
import redis, { redisReady } from "./redis.js";

import logger from "./logger.js";

const CONFIG_ENV =
  process.env.REDIS_ENV ||
  process.env.CONFIG_ENV ||
  process.env.NODE_ENV ||
  "development";
const CONFIG_REDIS_HASH_KEY =
  process.env.CONFIG_REDIS_HASH_KEY ||
  (CONFIG_ENV === "production"
    ? "notification_production"
    : `notification_${CONFIG_ENV}`);

function boolValue(value, defaultValue = true) {
  if (value === undefined || value === "") return defaultValue;
  return String(value).toLowerCase() === "true" || value === "1";
}

function numberValue(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseBrokers(raw) {
  if (!raw || !String(raw).trim()) {
    return ["localhost:9098", "kafka:9098"];
  }
  return String(raw)
    .split(",")
    .map((b) => b.trim())
    .filter(Boolean);
}

function parseDeviceTokens(raw) {
  if (!raw || !String(raw).trim()) return [];
  return String(raw)
    .split(/[\s,]+/)
    .map((t) => t.trim())
    .filter(Boolean);
}

function normalizeConfigKeys(source = {}) {
  return {
    nodeEnv: source.nodeEnv || source.NODE_ENV,
    kafkaBrokers: source.kafkaBrokers || source.KAFKA_BROKERS,
    kafkaClientId: source.kafkaClientId || source.KAFKA_CLIENT_ID,
    kafkaGroupId: source.kafkaGroupId || source.KAFKA_GROUP_ID,
    kafkaTopic: source.kafkaTopic || source.KAFKA_TOPIC,
    kafkaFromBeginning:
      source.kafkaFromBeginning || source.KAFKA_FROM_BEGINNING,
    emailEnabled: source.emailEnabled || source.EMAIL_ENABLED,
    smtpHost: source.smtpHost || source.SMTP_HOST,
    smtpPort: source.smtpPort || source.SMTP_PORT,
    smtpSecure: source.smtpSecure || source.SMTP_SECURE,
    smtpUser: source.smtpUser || source.SMTP_USER,
    smtpPassword:
      source.smtpPassword || source.SMTP_PASSWORD || source.GMAIL_APP_PASSWORD,
    emailFrom: source.emailFrom || source.EMAIL_FROM,
    emailTo: source.emailTo || source.EMAIL_TO,
    whatsappEnabled: source.whatsappEnabled || source.WHATSAPP_ENABLED,
    twilioAccountSid: source.twilioAccountSid || source.TWILIO_ACCOUNT_SID,
    twilioAuthToken: source.twilioAuthToken || source.TWILIO_AUTH_TOKEN,
    twilioWhatsappFrom:
      source.twilioWhatsappFrom || source.TWILIO_WHATSAPP_FROM,
    whatsappTo: source.whatsappTo || source.WHATSAPP_TO,
    fcmEnabled: source.fcmEnabled || source.FCM_ENABLED,
    firebaseServiceAccountPath:
      source.firebaseServiceAccountPath || source.FIREBASE_SERVICE_ACCOUNT_PATH,
    firebaseServiceAccountJson:
      source.firebaseServiceAccountJson || source.FIREBASE_SERVICE_ACCOUNT_JSON,
    fcmDeviceTokens: source.fcmDeviceTokens || source.FCM_DEVICE_TOKENS,
    kafkaUsername: source.kafkaUsername || source.KAFKA_USERNAME,
    kafkaPassword: source.kafkaPassword || source.KAFKA_PASSWORD,
  };
}

function buildConfig(overrides = {}) {
  const normalized = normalizeConfigKeys(overrides);
  const kafkaTopic = (
    normalized.kafkaTopic ||
    process.env.KAFKA_TOPIC ||
    "backend-topic"
  )
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);

  return {
    nodeEnv: normalized.nodeEnv || process.env.NODE_ENV || CONFIG_ENV,
    configRedisHashKey: CONFIG_REDIS_HASH_KEY,
    kafka: {
      brokers: parseBrokers(
        normalized.kafkaBrokers || process.env.KAFKA_BROKERS,
      ),
      clientId:
        normalized.kafkaClientId ||
        process.env.KAFKA_CLIENT_ID ||
        "port-notification-service",
      groupId:
        normalized.kafkaGroupId ||
        process.env.KAFKA_GROUP_ID ||
        "port-notification-consumer-group",
      topic: kafkaTopic,
      fromBeginning: boolValue(
        normalized.kafkaFromBeginning || process.env.KAFKA_FROM_BEGINNING,
        false,
      ),
      username: process.env.KAFKA_USERNAME || normalized.kafkaUsername || "",
      password: process.env.KAFKA_PASSWORD || normalized.kafkaPassword || "",
    },
    email: {
      enabled: boolValue(
        normalized.emailEnabled || process.env.EMAIL_ENABLED,
        true,
      ),
      host: normalized.smtpHost || process.env.SMTP_HOST || "smtp.gmail.com",
      port: numberValue(normalized.smtpPort || process.env.SMTP_PORT, 587),
      secure: boolValue(
        normalized.smtpSecure || process.env.SMTP_SECURE,
        false,
      ),
      user: normalized.smtpUser || process.env.SMTP_USER || "",
      pass:
        normalized.smtpPassword ||
        process.env.SMTP_PASSWORD ||
        process.env.GMAIL_APP_PASSWORD ||
        "",
      from:
        normalized.emailFrom ||
        process.env.EMAIL_FROM ||
        normalized.smtpUser ||
        process.env.SMTP_USER ||
        "",
      defaultTo: normalized.emailTo || process.env.EMAIL_TO || "",
    },
    whatsapp: {
      enabled: boolValue(
        normalized.whatsappEnabled || process.env.WHATSAPP_ENABLED,
        true,
      ),
      accountSid:
        normalized.twilioAccountSid || process.env.TWILIO_ACCOUNT_SID || "",
      authToken:
        normalized.twilioAuthToken || process.env.TWILIO_AUTH_TOKEN || "",
      from:
        normalized.twilioWhatsappFrom || process.env.TWILIO_WHATSAPP_FROM || "",
      defaultTo: normalized.whatsappTo || process.env.WHATSAPP_TO || "",
    },
    fcm: {
      enabled: boolValue(
        normalized.fcmEnabled || process.env.FCM_ENABLED,
        true,
      ),
      serviceAccountPath:
        normalized.firebaseServiceAccountPath ||
        process.env.FIREBASE_SERVICE_ACCOUNT_PATH ||
        "",
      serviceAccountJson:
        normalized.firebaseServiceAccountJson ||
        process.env.FIREBASE_SERVICE_ACCOUNT_JSON ||
        "",
      defaultTokens: parseDeviceTokens(
        normalized.fcmDeviceTokens || process.env.FCM_DEVICE_TOKENS || "",
      ),
    },
  };
}

let config = buildConfig();

export async function loadConfig() {
  try {
    console.log("redisConfig:1"); // ✅ Debug log to check what we got from Redis
    await redisReady; // ← wait for Redis before touching it

    const redisConfig = await redis.hgetall(CONFIG_REDIS_HASH_KEY);

    console.log("redisConfig:1"); // ✅ Debug log to check what we got from Redis

    if (!redisConfig || Object.keys(redisConfig).length === 0) {
      logger.warn(
        `Redis config missing for hash "${CONFIG_REDIS_HASH_KEY}". Using environment defaults.`,
      );
      return config;
    }

    console.log("redisConfig:", redisConfig); // ✅ Debug log to check what we got from Redis

    config = buildConfig(redisConfig);
    logger.info(`Loaded config from Redis hash "${CONFIG_REDIS_HASH_KEY}"`);
  } catch (error) {
    logger.error(
      `Unable to load Redis config. Using environment defaults: ${error.message}`,
    );
  }

  return config;
}

export const getConfig = () => config;

export default config;
