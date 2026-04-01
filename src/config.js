import "dotenv/config";

function boolEnv(name, defaultValue = true) {
  const v = process.env[name];
  if (v === undefined || v === "") return defaultValue;
  return String(v).toLowerCase() === "true" || v === "1";
}

function parseBrokers(raw) {
  if (!raw || !String(raw).trim()) {
    // support both host machine and Docker network names by default
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

export function loadConfig() {
  const kafkaBrokers = parseBrokers(process.env.KAFKA_BROKERS);
  const kafkaClientId =
    process.env.KAFKA_CLIENT_ID || "port-notification-service";
  const kafkaGroupId =
    process.env.KAFKA_GROUP_ID || "port-notification-consumer-group";
  const kafkaTopic = (process.env.KAFKA_TOPIC || "backend-topic").split(",").map(t => t.trim());
  const fromBeginning = boolEnv("KAFKA_FROM_BEGINNING", false);

  return {
    kafka: {
      brokers: kafkaBrokers,
      clientId: kafkaClientId,
      groupId: kafkaGroupId,
      topic: kafkaTopic,
      fromBeginning,
    },
    email: {
      enabled: boolEnv("EMAIL_ENABLED", true),
      host: process.env.SMTP_HOST || "smtp.gmail.com",
      port: Number(process.env.SMTP_PORT || "587"),
      secure: boolEnv("SMTP_SECURE", false),
      user: process.env.SMTP_USER || "",
      pass: process.env.SMTP_PASSWORD || process.env.GMAIL_APP_PASSWORD || "",
      from: process.env.EMAIL_FROM || process.env.SMTP_USER || "",
      defaultTo: process.env.EMAIL_TO || "",
    },
    whatsapp: {
      enabled: boolEnv("WHATSAPP_ENABLED", true),
      accountSid: process.env.TWILIO_ACCOUNT_SID || "",
      authToken: process.env.TWILIO_AUTH_TOKEN || "",
      from: process.env.TWILIO_WHATSAPP_FROM || "",
      defaultTo: process.env.WHATSAPP_TO || "",
    },
    fcm: {
      enabled: boolEnv("FCM_ENABLED", true),
      serviceAccountPath: process.env.FIREBASE_SERVICE_ACCOUNT_PATH || "",
      serviceAccountJson: process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "",
      defaultTokens: parseDeviceTokens(process.env.FCM_DEVICE_TOKENS || ""),
    },
  };
}
