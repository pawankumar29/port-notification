import nodemailer from "nodemailer";
import logger from "../logger.js";

export async function sendEmail(cfg, { to, subject, text, html }) {
  if (!cfg.enabled) {
    logger.debug("Email channel disabled; skipping send.");
    return;
  }
  const recipient = to || cfg.defaultTo;
  if (!recipient) {
    logger.warn("Email skipped: no recipient (set EMAIL_TO or payload.emailTo).");
    return;
  }
  if (!cfg.user || !cfg.pass) {
    logger.warn("Email skipped: SMTP_USER or SMTP_PASSWORD not set.");
    return;
  }

  const transporter = nodemailer.createTransport({
    host: cfg.host,
    port: cfg.port,
    secure: cfg.secure,
    auth: {
      user: cfg.user,
      pass: cfg.pass,
    },
  });

  await transporter.sendMail({
    from: cfg.from,
    to: recipient,
    subject: subject || "Notification",
    text: text || undefined,
    html: html || undefined,
  });

  logger.info(`Email sent to ${recipient}`);
}
