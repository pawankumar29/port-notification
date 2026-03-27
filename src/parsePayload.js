/**
 * Accepts Kafka message string; returns structured fields for email, WhatsApp, FCM.
 * Contact-style JSON: { name, email, budget, message } → builds subject/text/html.
 * Also: { subject, text, html, emailTo, whatsappTo, fcmTokens, fcmTitle, fcmBody, data }
 */
function normalizeFcmTokens(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) {
    return raw.map((t) => String(t).trim()).filter(Boolean);
  }
  return String(raw)
    .split(/[\s,]+/)
    .map((t) => t.trim())
    .filter(Boolean);
}

function contactFormSummary(obj) {
  const keys = ["name", "email", "budget", "message"];
  const hasAny = keys.some(
    (k) => obj[k] != null && String(obj[k]).trim() !== ""
  );
  if (!hasAny) return null;

  const lines = [];
  if (obj.name != null && String(obj.name).trim())
    lines.push(`Name: ${obj.name}`);
  if (obj.email != null && String(obj.email).trim())
    lines.push(`Email: ${obj.email}`);
  if (obj.budget != null && String(obj.budget).trim())
    lines.push(`Budget: ${obj.budget}`);
  if (obj.message != null && String(obj.message).trim())
    lines.push(`Message: ${obj.message}`);

  const text = lines.join("\n");
  const subject = obj.name
    ? `New contact: ${obj.name}`
    : "New contact form";

  const html = obj.html
    ? String(obj.html)
    : `<p>${lines.map((l) => l.replace(/</g, "&lt;")).join("<br/>")}</p>`;

  return { text, subject: String(subject), html };
}

export function parsePayload(raw) {
  const trimmed = String(raw ?? "").trim();
  const empty = {
    subject: "Notification",
    text: "",
    html: undefined,
    emailTo: undefined,
    whatsappTo: undefined,
    fcmTokens: [],
    fcmTitle: undefined,
    fcmBody: undefined,
    fcmData: undefined,
  };

  if (!trimmed) return empty;

  try {
    const obj = JSON.parse(trimmed);
    if (obj && typeof obj === "object" && !Array.isArray(obj)) {
      const contact = contactFormSummary(obj);

      const explicitText =
        obj.text != null || obj.body != null
          ? String(obj.text ?? obj.body ?? "")
          : "";
      let text = explicitText.trim()
        ? explicitText
        : contact
          ? contact.text
          : obj.message != null
            ? String(obj.message)
            : "";

      let subject = obj.subject || obj.title || "Notification";
      let html = obj.html != null ? String(obj.html) : undefined;

      if (contact) {
        if (!obj.subject && !obj.title) subject = contact.subject;
        if (!html && contact.html) html = contact.html;
      }

      const fcmTokens = normalizeFcmTokens(obj.fcmTokens ?? obj.fcmToken);

      return {
        subject: String(subject),
        text: text != null ? String(text) : "",
        html,
        emailTo: obj.emailTo,
        whatsappTo: obj.whatsappTo,
        fcmTokens,
        fcmTitle: obj.fcmTitle,
        fcmBody: obj.fcmBody,
        fcmData: obj.data && typeof obj.data === "object" ? obj.data : undefined,
      };
    }
  } catch {
    // not JSON
  }

  return {
    subject: "Notification",
    text: trimmed,
    html: undefined,
    emailTo: undefined,
    whatsappTo: undefined,
    fcmTokens: [],
    fcmTitle: undefined,
    fcmBody: undefined,
    fcmData: undefined,
  };
}
