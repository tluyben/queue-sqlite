import { QueueManager } from "../src/queue-manager";
import { addSeconds, subSeconds } from "date-fns";
import fs from "fs";
import path from "path";

describe("Queue System", () => {
  let manager: QueueManager;
  const dbPath = path.join(__dirname, "test.db");

  beforeEach(() => {
    // Clean up any existing test database
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
    manager = new QueueManager(dbPath, 2); // Use 2 workers for testing
  });

  afterEach(async () => {
    // Clean up
    manager.stop();
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  describe("Basic Queue Operations", () => {
    test("should process an immediate message", async () => {
      const processed = jest.fn();

      manager.addListener("test", async (payload) => {
        processed(payload);
      });

      manager.start();

      await manager.addMessage({
        queue: "test",
        payload: { data: "test" },
      });

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 1000));

      expect(processed).toHaveBeenCalledWith({ data: "test" });
    });

    test("should handle multiple messages in order", async () => {
      const processed: any[] = [];

      manager.addListener("test", async (payload) => {
        processed.push(payload);
      });

      manager.start();

      await manager.addMessage({
        queue: "test",
        payload: { id: 1 },
      });

      await manager.addMessage({
        queue: "test",
        payload: { id: 2 },
      });

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 1000));

      expect(processed).toHaveLength(2);
      expect(processed[0].id).toBe(1);
      expect(processed[1].id).toBe(2);
    });
  });

  describe("Delayed Messages", () => {
    test("should process delayed message after specified time", async () => {
      const processed = jest.fn();

      manager.addListener("test", async (payload) => {
        processed(payload);
      });

      manager.start();

      const startAt = addSeconds(new Date(), 2);

      await manager.addMessage({
        queue: "test",
        payload: { data: "delayed" },
        startAt,
      });

      // Check not processed immediately
      await new Promise((resolve) => setTimeout(resolve, 1000));
      expect(processed).not.toHaveBeenCalled();

      // Check processed after delay
      await new Promise((resolve) => setTimeout(resolve, 2000));
      expect(processed).toHaveBeenCalledWith({ data: "delayed" });
    });
  });

  describe("Retry Mechanism", () => {
    test("should retry failed messages", async () => {
      let attempts = 0;

      manager.addListener("test", async (payload) => {
        attempts++;
        if (attempts <= 2) {
          throw new Error("Simulated failure");
        }
      });

      manager.start();

      await manager.addMessage({
        queue: "test",
        payload: { data: "retry-test" },
        maxRetries: 3,
      });

      // Wait for all retry attempts
      await new Promise((resolve) => setTimeout(resolve, 3000));

      expect(attempts).toBe(3);
    });

    test("should stop retrying after max retries", async () => {
      let attempts = 0;

      manager.addListener("test", async () => {
        attempts++;
        throw new Error("Simulated failure");
      });

      manager.start();

      await manager.addMessage({
        queue: "test",
        payload: { data: "retry-test" },
        maxRetries: 2,
      });

      // Wait for all retry attempts
      await new Promise((resolve) => setTimeout(resolve, 3000));

      expect(attempts).toBe(3); // Initial attempt + 2 retries
    });
  });

  describe("Interval Messages", () => {
    test("should process interval message multiple times", async () => {
      const processed = jest.fn();

      manager.addListener("test", async (payload) => {
        processed(payload);
      });

      manager.start();

      await manager.addMessage({
        queue: "test",
        payload: { data: "interval" },
        interval: 1000, // 1 second interval
        maxRespawns: 2, // Run 3 times total (initial + 2 respawns)
      });

      // Wait for all executions
      await new Promise((resolve) => setTimeout(resolve, 3500));

      expect(processed).toHaveBeenCalledTimes(3);
    });
  });

  describe("Cron Messages", () => {
    test("should process cron message at scheduled time", async () => {
      const processed = jest.fn();

      manager.addListener("test", async (payload) => {
        processed(payload);
      });

      manager.start();

      // Schedule for next minute
      const cronPattern = "* * * * *";

      await manager.addMessage({
        queue: "test",
        payload: { data: "cron" },
        cronPattern,
      });

      // Wait until next minute
      const now = new Date();
      const waitTime =
        60000 - (now.getSeconds() * 1000 + now.getMilliseconds()) + 1000;
      await new Promise((resolve) => setTimeout(resolve, waitTime));

      expect(processed).toHaveBeenCalledWith({ data: "cron" });
    });
  });

  describe("Multiple Queues", () => {
    test("should handle multiple queues independently", async () => {
      const queue1Processed = jest.fn();
      const queue2Processed = jest.fn();

      manager.addListener("queue1", async (payload) => {
        queue1Processed(payload);
      });

      manager.addListener("queue2", async (payload) => {
        queue2Processed(payload);
      });

      manager.start();

      await manager.addMessage({
        queue: "queue1",
        payload: { data: "queue1" },
      });

      await manager.addMessage({
        queue: "queue2",
        payload: { data: "queue2" },
      });

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 1000));

      expect(queue1Processed).toHaveBeenCalledWith({ data: "queue1" });
      expect(queue2Processed).toHaveBeenCalledWith({ data: "queue2" });
    });
  });

  describe("Multiple Listeners", () => {
    test("should execute all listeners for a queue", async () => {
      const listener1 = jest.fn();
      const listener2 = jest.fn();

      manager.addListener("test", async (payload) => {
        listener1(payload);
      });

      manager.addListener("test", async (payload) => {
        listener2(payload);
      });

      manager.start();

      await manager.addMessage({
        queue: "test",
        payload: { data: "multi-listener" },
      });

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 1000));

      expect(listener1).toHaveBeenCalledWith({ data: "multi-listener" });
      expect(listener2).toHaveBeenCalledWith({ data: "multi-listener" });
    });
  });

  describe("Error Handling", () => {
    test("should handle listener errors gracefully", async () => {
      const errorListener = jest.fn();

      manager.addListener("test", async () => {
        throw new Error("Simulated error");
      });

      manager.start();

      await manager.addMessage({
        queue: "test",
        payload: { data: "error-test" },
      });

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 1000));

      expect(errorListener).toHaveBeenCalled();
    });
  });

  describe("Concurrent Processing", () => {
    test("should process messages concurrently with multiple workers", async () => {
      const processed: any[] = [];
      const processTime = 1000; // 1 second processing time

      manager.addListener("test", async (payload) => {
        await new Promise((resolve) => setTimeout(resolve, processTime));
        processed.push(payload);
      });

      manager.start();

      const startTime = Date.now();

      // Add multiple messages
      await Promise.all([
        manager.addMessage({
          queue: "test",
          payload: { id: 1 },
        }),
        manager.addMessage({
          queue: "test",
          payload: { id: 2 },
        }),
      ]);

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, processTime * 1.5));

      const endTime = Date.now();

      expect(processed).toHaveLength(2);
      // Should take less than 2x process time due to concurrent processing
      expect(endTime - startTime).toBeLessThan(processTime * 2);
    });
  });
});
