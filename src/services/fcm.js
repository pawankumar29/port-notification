import admin from "firebase-admin";
import fs from "fs";
import logger from "../logger.js";

let initialized = false;

function loadServiceAccount(cfg) {
  const filePath =
    cfg.serviceAccountPath ||
    process.env.GOOGLE_APPLICATION_CREDENTIALS ||
    "";
  if (filePath && fs.existsSync(filePath)) {
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  }
  if (cfg.serviceAccountJson && String(cfg.serviceAccountJson).trim()) {
    return JSON.parse(cfg.serviceAccountJson);
  }
  return null;
}

function ensureApp(cfg) {
  if (!cfg.enabled) return null;
  if (initialized) return admin.app();

  const sa = loadServiceAccount(cfg);
  if (!sa) {
    logger.warn(
      "FCM: enabled but missing credentials (set FIREBASE_SERVICE_ACCOUNT_PATH or FIREBASE_SERVICE_ACCOUNT_JSON, or GOOGLE_APPLICATION_CREDENTIALS)"
    );
    return null;
  }
  if (sa.private_key && typeof sa.private_key === "string") {
    sa.private_key = sa.private_key.replace(/\\n/g, "\n");
  }

  admin.initializeApp({ credential: admin.credential.cert(sa) });
  initialized = true;
  logger.info("Firebase Admin initialized for FCM");
  return admin.app();
}

function stringifyData(data) {
  if (!data || typeof data !== "object") return undefined;
  const out = {};
  for (const [k, v] of Object.entries(data)) {
    if (v === undefined || v === null) continue;
    out[k] = String(v);
  }
  return Object.keys(out).length ? out : undefined;
}

const MULTICAST_LIMIT = 500;

export async function sendFcmPush(cfg, { tokens, title, body, data }) {
  if (!cfg.enabled) {
    logger.debug("FCM disabled; skipping push.");
    return;
  }

  const app = ensureApp(cfg);
  if (!app) return;

  const tokenList = [...new Set((tokens || []).filter(Boolean))];
  if (tokenList.length === 0) {
    logger.warn(
      "FCM skipped: no device tokens (set FCM_DEVICE_TOKENS in .env or send fcmTokens in Kafka payload)"
    );
    return;
  }

  const messaging = admin.messaging();
  const notification = {
    title: title || "Notification",
    body: (body || "").slice(0, 4000),
  };
  const dataPayload = stringifyData(data);

  for (let i = 0; i < tokenList.length; i += MULTICAST_LIMIT) {
    const chunk = tokenList.slice(i, i + MULTICAST_LIMIT);
    const res = await messaging.sendEachForMulticast({
      tokens: chunk,
      notification,
      data: dataPayload,
    });
    logger.info(
      `FCM batch: success=${res.successCount} failure=${res.failureCount}`
    );
    res.responses.forEach((r, idx) => {
      if (!r.success && r.error) {
        logger.error(
          `FCM token error [${chunk[idx]?.slice(0, 12)}…]: ${r.error.code} ${r.error.message}`
        );
      }
    });
  }
}
