import Database from "better-sqlite3";
import { QueueMessage } from "./types";

export class QueueDatabase {
  private db: Database.Database;

  constructor(dbPath: string) {
    this.db = new Database(dbPath);
    this.initialize();
  }

  private initialize() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS queue_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        queue TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        startAt DATETIME NOT NULL,
        retryCount INTEGER DEFAULT 0,
        maxRetries INTEGER DEFAULT 0,
        interval INTEGER,
        respawnCount INTEGER DEFAULT -1,
        maxRespawns INTEGER DEFAULT -1,
        cronPattern TEXT,
        createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
        updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
      );
      
      CREATE INDEX IF NOT EXISTS idx_queue_status ON queue_messages(queue, status);
      CREATE INDEX IF NOT EXISTS idx_start_at ON queue_messages(startAt);
    `);
  }

  addMessage(
    message: Omit<QueueMessage, "id" | "createdAt" | "updatedAt">
  ): number {
    const stmt = this.db.prepare(`
      INSERT INTO queue_messages (
        queue, payload, status, startAt, retryCount, maxRetries,
        interval, respawnCount, maxRespawns, cronPattern
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const result = stmt.run(
      message.queue,
      message.payload,
      message.status,
      message.startAt.toISOString(),
      message.retryCount,
      message.maxRetries,
      message.interval,
      message.respawnCount,
      message.maxRespawns,
      message.cronPattern
    );

    return result.lastInsertRowid as number;
  }

  getNextMessage(workerId: number): QueueMessage | undefined {
    const now = new Date().toISOString();

    return this.db.transaction(() => {
      const message = this.db
        .prepare(
          `
        SELECT * FROM queue_messages
        WHERE status = 'pending'
        AND startAt <= ?
        ORDER BY startAt ASC
        LIMIT 1
      `
        )
        .get(now) as QueueMessage | undefined;

      if (message) {
        this.db
          .prepare(
            `
          UPDATE queue_messages
          SET status = 'processing',
              updatedAt = CURRENT_TIMESTAMP
          WHERE id = ?
        `
          )
          .run(message.id);
      }

      return message;
    })();
  }

  markMessageComplete(id: number): void {
    this.db
      .prepare(
        `
      UPDATE queue_messages
      SET status = 'completed',
          updatedAt = CURRENT_TIMESTAMP
      WHERE id = ?
    `
      )
      .run(id);
  }

  markMessageFailed(id: number): void {
    const message = this.db
      .prepare("SELECT * FROM queue_messages WHERE id = ?")
      .get(id) as QueueMessage;

    if (message.retryCount < message.maxRetries) {
      this.db
        .prepare(
          `
        UPDATE queue_messages
        SET status = 'pending',
            retryCount = retryCount + 1,
            updatedAt = CURRENT_TIMESTAMP
        WHERE id = ?
      `
        )
        .run(id);
    } else {
      this.db
        .prepare(
          `
        UPDATE queue_messages
        SET status = 'failed',
            updatedAt = CURRENT_TIMESTAMP
        WHERE id = ?
      `
        )
        .run(id);
    }
  }

  rescheduleMessage(id: number, nextRunTime: Date): void {
    this.db
      .prepare(
        `
      UPDATE queue_messages
      SET startAt = ?,
          status = 'pending',
          updatedAt = CURRENT_TIMESTAMP
      WHERE id = ?
    `
      )
      .run(nextRunTime.toISOString(), id);
  }
}
