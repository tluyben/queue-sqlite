import { EventEmitter } from "events";

export type MessageHandler = (payload: any) => Promise<void>;

export interface QueueListener {
  queue: string;
  handler: MessageHandler;
}

export class QueueEventEmitter extends EventEmitter {
  addQueueListener(queue: string, handler: MessageHandler): void {
    this.on(`queue:${queue}`, handler);
  }

  removeQueueListener(queue: string, handler: MessageHandler): void {
    this.off(`queue:${queue}`, handler);
  }

  emitQueueEvent(queue: string, payload: any): void {
    this.emit(`queue:${queue}`, payload);
  }
}
export interface QueueMessage {
  id?: number;
  queue: string;
  payload: string;
  status: "pending" | "processing" | "completed" | "failed" | "scheduled";
  startAt: Date;
  retryCount: number;
  maxRetries: number;
  interval?: number;
  respawnCount: number;
  maxRespawns: number;
  cronPattern?: string;
  createdAt: Date;
  updatedAt: Date;
}
