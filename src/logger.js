import winston from "winston";
import * as stackTrace from "stack-trace";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const logDir = path.resolve(__dirname, "..", "logs");
if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

const getFileLine = () => {
    const trace = stackTrace.get();
    const caller = trace.find(f => {
        const file = f.getFileName() || '';
        return (
            !file.includes('node_modules') &&
            !file.includes('logger.js') &&
            !file.includes('winston') &&
            file.endsWith('.js')
        );
    });

    if (!caller) return 'unknown:0';

    const fileName = path.basename(caller.getFileName());
    const line = caller.getLineNumber();

    return `${fileName}:${line}`;
};

const level = process.env.LOG_LEVEL || "info";

const logger = winston.createLogger({
  level,
  format: winston.format.combine(
    winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
    winston.format.printf(({ level: lvl, message, timestamp }) => {
      return `[${timestamp}] [${lvl.toUpperCase()}] ${getFileLine()} - ${message}`;
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

// Convenience methods
logger.info = (msg) => logger.log({ level: 'info', message: msg });
logger.debug = (msg) => logger.log({ level: 'debug', message: msg });
logger.warn = (msg) => logger.log({ level: 'warn', message: msg });
logger.error = (msg) => logger.log({ level: 'error', message: msg });

export default logger;
