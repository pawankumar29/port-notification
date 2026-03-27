import twilio from "twilio";
import logger from "../logger.js";

export async function sendWhatsApp(cfg, { to, body }) {
  if (!cfg.enabled) {
    logger.debug("WhatsApp channel disabled; skipping send.");
    return;
  }
  const recipient = to || cfg.defaultTo;
  if (!recipient) {
    logger.warn(
      "WhatsApp skipped: no recipient (set WHATSAPP_TO or payload.whatsappTo)."
    );
    return;
  }
  if (!cfg.accountSid || !cfg.authToken || !cfg.from) {
    logger.warn(
      "WhatsApp skipped: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, or TWILIO_WHATSAPP_FROM not set."
    );
    return;
  }

  const client = twilio(cfg.accountSid, cfg.authToken);
  const messageBody = body || "";

  await client.messages.create({
    from: cfg.from,
    to: recipient.startsWith("whatsapp:") ? recipient : `whatsapp:${recipient}`,
    body: messageBody,
  });

  logger.info(`WhatsApp message sent to ${recipient}`);
}
