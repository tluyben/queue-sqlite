import { parentPort } from "worker_threads";
import { QueueDatabase } from "./database";
import { QueueMessage } from "./types";
import cronParser from "cron-parser";
import { addMilliseconds } from "date-fns";

class QueueWorker {
  private db: QueueDatabase;
  private workerId: number;

  constructor(dbPath: string, workerId: number) {
    this.db = new QueueDatabase(dbPath);
    this.workerId = workerId;
  }

  async start() {
    while (true) {
      const message = this.db.getNextMessage(this.workerId);

      if (message) {
        try {
          await this.processMessage(message);
          this.db.markMessageComplete(message.id!);

          // Handle interval-based messages
          if (message.interval) {
            if (
              message.maxRespawns === -1 ||
              message.respawnCount < message.maxRespawns
            ) {
              const nextRunTime = addMilliseconds(new Date(), message.interval);
              this.db.rescheduleMessage(message.id!, nextRunTime);
            }
          }

          // Handle cron-based messages
          if (message.cronPattern) {
            const interval = cronParser.parseExpression(message.cronPattern);
            const nextRunTime = interval.next().toDate();
            this.db.rescheduleMessage(message.id!, nextRunTime);
          }
        } catch (error) {
          this.db.markMessageFailed(message.id!);
        }
      }

      // Small delay to prevent CPU spinning
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  private async processMessage(message: QueueMessage): Promise<void> {
    const payload = JSON.parse(message.payload);

    // Send the message to the parent thread for processing
    parentPort!.postMessage({
      type: "process",
      queue: message.queue,
      payload,
    });

    // Wait for processing result
    const result = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("Message processing timeout"));
      }, 30000); // 30 second timeout

      const handler = (data: any) => {
        if (data.type === "processResult" && data.messageId === message.id) {
          clearTimeout(timeout);
          if (data.error) {
            reject(new Error(data.error));
          } else {
            resolve(data.result);
          }
          parentPort!.off("message", handler);
        }
      };

      parentPort!.on("message", handler);
    });

    return result as void;
  }
}

if (parentPort) {
  const worker = new QueueWorker(
    process.env.DB_PATH!,
    parseInt(process.env.WORKER_ID!)
  );
  worker.start().catch(console.error);
}
