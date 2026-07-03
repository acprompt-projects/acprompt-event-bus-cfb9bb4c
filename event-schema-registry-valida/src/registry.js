const Ajv = require("ajv");
const addFormats = require("ajv-formats");

class EventSchemaRegistry {
  constructor(options = {}) {
    this.ajv = new Ajv({ allErrors: true, strict: false });
    addFormats(this.ajv);
    this.schemas = new Map();
    this.validators = new Map();
    this.violationListeners = [];
    this.allowUnregistered = options.allowUnregistered ?? false;
  }

  register(eventType, schema, metadata = {}) {
    if (this.schemas.has(eventType)) {
      throw new Error(`Schema already registered for event type: "${eventType}"`);
    }
    this.ajv.addSchema(schema, eventType);
    const validate = this.ajv.getSchema(eventType);
    this.schemas.set(eventType, { schema, metadata, registeredAt: new Date().toISOString() });
    this.validators.set(eventType, validate);
    return { eventType, registeredAt: this.schemas.get(eventType).registeredAt };
  }

  unregister(eventType) {
    if (!this.schemas.has(eventType)) return false;
    this.schemas.delete(eventType);
    this.validators.delete(eventType);
    this.ajv.removeSchema(eventType);
    return true;
  }

  update(eventType, schema, metadata = {}) {
    if (!this.schemas.has(eventType)) {
      throw new Error(`No schema registered for event type: "${eventType}"`);
    }
    this.ajv.removeSchema(eventType);
    this.ajv.addSchema(schema, eventType);
    const validate = this.ajv.getSchema(eventType);
    const prev = this.schemas.get(eventType);
    this.schemas.set(eventType, {
      schema,
      metadata: { ...prev.metadata, ...metadata },
      registeredAt: prev.registeredAt,
      updatedAt: new Date().toISOString(),
    });
    this.validators.set(eventType, validate);
    return { eventType, updatedAt: this.schemas.get(eventType).updatedAt };
  }

  validate(eventType, payload) {
    const registered = this.schemas.has(eventType);
    if (!registered) {
      if (this.allowUnregistered) {
        return { valid: true, eventType, errors: null, unregistered: true };
      }
      const violation = this._buildViolation(eventType, payload, [
        { message: `Unknown event type: "${eventType}"`, field: "eventType" },
      ]);
      this._emitViolation(violation);
      return { valid: false, eventType, errors: violation.errors, unregistered: false };
    }
    const validate = this.validators.get(eventType);
    const valid = validate(payload);
    if (!valid) {
      const errors = validate.errors.map((e) => ({
        field: e.instancePath || e.params?.missingProperty || "/",
        message: e.message,
        keyword: e.keyword,
        schemaPath: e.schemaPath,
      }));
      const violation = this._buildViolation(eventType, payload, errors);
      this._emitViolation(violation);
      return { valid: false, eventType, errors, unregistered: false };
    }
    return { valid: true, eventType, errors: null, unregistered: false };
  }

  has(eventType) {
    return this.schemas.has(eventType);
  }

  get(eventType) {
    return this.schemas.has(eventType) ? { ...this.schemas.get(eventType) } : null;
  }

  list() {
    return Array.from(this.schemas.entries()).map(([type, info]) => ({
      eventType: type,
      metadata: info.metadata,
      registeredAt: info.registeredAt,
      updatedAt: info.updatedAt || null,
    }));
  }

  onViolation(listener) {
    if (typeof listener !== "function") throw new TypeError("Listener must be a function");
    this.violationListeners.push(listener);
    return () => {
      this.violationListeners = this.violationListeners.filter((l) => l !== listener);
    };
  }

  _buildViolation(eventType, payload, errors) {
    return {
      schema: "schema-violation",
      version: 1,
      timestamp: new Date().toISOString(),
      data: {
        eventType,
        payload,
        errors,
        errorCount: errors.length,
      },
    };
  }

  _emitViolation(violation) {
    for (const listener of this.violationListeners) {
      try {
        listener(violation);
      } catch (_) {
        // swallow listener errors to avoid disrupting the bus
      }
    }
  }
}

// Pre-built schemas for known ACPrompt event types
const BUILTIN_SCHEMAS = {
  "task.claimed": {
    type: "object",
    required: ["taskId", "projectId", "agentId", "claimedAt"],
    properties: {
      taskId: { type: "string", minLength: 1 },
      projectId: { type: "string", minLength: 1 },
      agentId: { type: "string", minLength: 1 },
      claimedAt: { type: "string", format: "date-time" },
      priority: { type: "string", enum: ["low", "medium", "high", "critical"] },
    },
    additionalProperties: false,
  },
  "task.completed": {
    type: "object",
    required: ["taskId", "projectId", "agentId", "completedAt", "status"],
    properties: {
      taskId: { type: "string", minLength: 1 },
      projectId: { type: "string", minLength: 1 },
      agentId: { type: "string", minLength: 1 },
      completedAt: { type: "string", format: "date-time" },
      status: { type: "string", enum: ["success", "failure", "partial"] },
      summary: { type: "string", maxLength: 2048 },
    },
    additionalProperties: false,
  },
  "note.posted": {
    type: "object",
    required: ["noteId", "projectId", "authorId", "content", "postedAt"],
    properties: {
      noteId: { type: "string", minLength: 1 },
      projectId: { type: "string", minLength: 1 },
      authorId: { type: "string", minLength: 1 },
      content: { type: "string", minLength: 1, maxLength: 16384 },
      postedAt: { type: "string", format: "date-time" },
      tags: { type: "array", items: { type: "string" } },
    },
    additionalProperties: false,
  },
  "invite.sent": {
    type: "object",
    required: ["inviteId", "projectId", "senderId", "recipientId", "sentAt"],
    properties: {
      inviteId: { type: "string", minLength: 1 },
      projectId: { type: "string", minLength: 1 },
      senderId: { type: "string", minLength: 1 },
      recipientId: { type: "string", minLength: 1 },
      sentAt: { type: "string", format: "date-time" },
      role: { type: "string", enum: ["viewer", "editor", "admin"] },
      expiresAt: { type: "string", format: "date-time" },
    },
    additionalProperties: false,
  },
};

function createRegistry(options = {}) {
  const registry = new EventSchemaRegistry(options);
  if (options.loadBuiltins !== false) {
    for (const [type, schema] of Object.entries(BUILTIN_SCHEMAS)) {
      registry.register(type, schema, { builtin: true });
    }
  }
  return registry;
}

module.exports = { EventSchemaRegistry, BUILTIN_SCHEMAS, createRegistry };