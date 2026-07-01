import { EventEmitter } from "events";

export type ProjectEventType =
  | "task:claimed"
  | "task:completed"
  | "note:posted"
  | "invite:sent";

export interface ProjectEvent {
  id: string;
  type: ProjectEventType;
  projectId: string;
  payload: Record<string, unknown>;
  timestamp: number;
}

export type Subscriber = (event: ProjectEvent) => void | Promise<void>;
export type Middleware = (
  event: ProjectEvent,
  next: (evt: ProjectEvent) => void
) => void | Promise<void>;

let idCounter = 0;
function generateId(): string {
  return `evt_${Date.now()}_${++idCounter}`;
}

export class EventBus extends EventEmitter {
  private subscribers: Map<ProjectEventType, Set<Subscriber>> = new Map();
  private wildcardSubscribers: Set<Subscriber> = new Set();
  private beforeMiddlewares: Middleware[] = [];
  private afterMiddlewares: Middleware[] = [];
  private history: ProjectEvent[] = [];
  private maxHistory: number;

  constructor(maxHistory = 1000) {
    super();
    this.maxHistory = maxHistory;
  }

  /** Register a before-publish middleware. Chain order = registration order. */
  useBefore(mw: Middleware): this {
    this.beforeMiddlewares.push(mw);
    return this;
  }

  /** Register an after-publish middleware. */
  useAfter(mw: Middleware): this {
    this.afterMiddlewares.push(mw);
    return this;
  }

  /** Subscribe to a specific event type. Returns unsubscribe function. */
  subscribe(type: ProjectEventType, fn: Subscriber): () => void {
    if (!this.subscribers.has(type)) {
      this.subscribers.set(type, new Set());
    }
    this.subscribers.get(type)!.add(fn);
    return () => {
      this.subscribers.get(type)?.delete(fn);
    };
  }

  /** Subscribe to ALL event types. Returns unsubscribe function. */
  subscribeAll(fn: Subscriber): () => void {
    this.wildcardSubscribers.add(fn);
    return () => {
      this.wildcardSubscribers.delete(fn);
    };
  }

  /** Publish an event through the middleware chain to all subscribers. */
  async publish(
    type: ProjectEventType,
    projectId: string,
    payload: Record<string, unknown> = {}
  ): Promise<ProjectEvent> {
    const event: ProjectEvent = {
      id: generateId(),
      type,
      projectId,
      payload,
      timestamp: Date.now(),
    };

    // Before middleware chain
    let current = 0;
    const beforeMws = this.beforeMiddlewares;
    const self = this;

    await new Promise<void>((resolve, reject) => {
      function runBefore(evt: ProjectEvent): void {
        if (current >= beforeMws.length) {
          resolve();
          return;
        }
        const mw = beforeMws[current++];
        try {
          const result = mw(evt, runBefore);
          if (result && typeof result === "object" && "then" in result) {
            result.catch(reject);
          }
        } catch (err) {
          reject(err);
        }
      }
      runBefore(event);
    }).catch((err) => {
      self.emit("error", err);
      throw err;
    });

    // Deliver to type-specific subscribers
    await this.deliver(event);

    // After middleware chain
    current = 0;
    const afterMws = this.afterMws ?? this.afterMiddlewares;
    await new Promise<void>((resolve) => {
      function runAfter(evt: ProjectEvent): void {
        if (current >= afterMws.length) {
          resolve();
          return;
        }
        const mw = afterMws[current++];
        try {
          const result = mw(evt, runAfter);
          if (result && typeof result === "object" && "then" in result) {
            result.catch(() => runAfter(evt));
          }
        } catch {
          runAfter(evt);
        }
      }
      runAfter(event);
    });

    // Store in history
    this.history.push(event);
    if (this.history.length > this.maxHistory) {
      this.history.shift();
    }

    this.emit("published", event);
    return event;
  }

  private async deliver(event: ProjectEvent): Promise<void> {
    const targets: Subscriber[] = [
      ...(this.subscribers.get(event.type) ?? []),
      ...this.wildcardSubscribers,
    ];

    const results = targets.map(async (fn) => {
      try {
        await fn(event);
      } catch (err) {
        this.emit("subscriber:error", err, event, fn);
      }
    });

    await Promise.all(results);
  }

  /** Retrieve recent event history, optionally filtered by type or project. */
  getHistory(filter?: {
    type?: ProjectEventType;
    projectId?: string;
    limit?: number;
  }): ProjectEvent[] {
    let result = this.history;
    if (filter?.type) result = result.filter((e) => e.type === filter.type);
    if (filter?.projectId)
      result = result.filter((e) => e.projectId === filter.projectId);
    if (filter?.limit) result = result.slice(-filter.limit);
    return result;
  }

  /** Remove all subscribers and middlewares. */
  reset(): void {
    this.subscribers.clear();
    this.wildcardSubscribers.clear();
    this.beforeMiddlewares = [];
    this.afterMiddlewares = [];
    this.history = [];
    this.removeAllListeners();
  }
}