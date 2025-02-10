import { Worker } from "worker_threads";
import path from "path";
import { QueueDatabase } from "./database";
import { QueueMessage, QueueEventEmitter, MessageHandler } from "./types";

export class QueueManager {
  private workers: Worker[] = [];
  private db: QueueDatabase;
  private eventEmitter: QueueEventEmitter;

  constructor(private dbPath: string, private numWorkers: number) {
    this.db = new QueueDatabase(dbPath);
    this.eventEmitter = new QueueEventEmitter();
  }

  addListener(queue: string, handler: MessageHandler): void {
    this.eventEmitter.addQueueListener(queue, handler);
  }

  removeListener(queue: string, handler: MessageHandler): void {
    this.eventEmitter.removeQueueListener(queue, handler);
  }

  start() {
    for (let i = 0; i < this.numWorkers; i++) {
      const worker = new Worker(
        path.join(__dirname, "..", "dist", "worker.js"),
        {
          env: {
            DB_PATH: this.dbPath,
            WORKER_ID: i.toString(),
          },
        }
      );

      worker.on("error", (error) => {
        console.error(`Worker ${i} error:`, error);
      });

      worker.on("message", async (data) => {
        if (data.type === "process") {
          try {
            // Process the message using registered handlers
            await this.eventEmitter.emitQueueEvent(data.queue, data.payload);
            worker.postMessage({
              type: "processResult",
              messageId: data.messageId,
              result: true,
            });
          } catch (error: any) {
            worker.postMessage({
              type: "processResult",
              messageId: data.messageId,
              error: error.message,
            });
          }
        }
      });

      this.workers.push(worker);
    }
  }

  async addMessage(options: {
    queue: string;
    payload: any;
    startAt?: Date;
    maxRetries?: number;
    interval?: number;
    maxRespawns?: number;
    cronPattern?: string;
  }): Promise<number> {
    const message: Omit<QueueMessage, "id" | "createdAt" | "updatedAt"> = {
      queue: options.queue,
      payload: JSON.stringify(options.payload),
      status: "pending",
      startAt: options.startAt || new Date(),
      retryCount: 0,
      maxRetries: options.maxRetries || 0,
      interval: options.interval,
      respawnCount: 0,
      maxRespawns: options.maxRespawns || -1,
      cronPattern: options.cronPattern,
    };

    return this.db.addMessage(message);
  }

  stop() {
    this.workers.forEach((worker) => worker.terminate());
  }
}
