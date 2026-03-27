import winston from "winston";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const logDir = path.resolve(__dirname, "..", "logs");
if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

const level = process.env.LOG_LEVEL || "info";

const logger = winston.createLogger({
  level,
  format: winston.format.combine(
    winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
    winston.format.printf(({ level: lvl, message, timestamp }) => {
      return `[${timestamp}] [${lvl.toUpperCase()}] ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({
      filename: path.join(logDir, "combined.log"),
    }),
    new winston.transports.File({
      filename: path.join(logDir, "error.log"),
      level: "error",
    }),
  ],
});

export default logger;
