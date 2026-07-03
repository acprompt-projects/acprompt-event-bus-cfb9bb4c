const { createRegistry, BUILTIN_SCHEMAS } = require("./registry");

let passed = 0;
let failed = 0;
function assert(cond, msg) {
  if (cond) { passed++; }
  else { failed++; console.error("  FAIL:", msg); }
}

// --- Setup ---
const registry = createRegistry();
const violations = [];
registry.onViolation((v) => violations.push(v));

// --- Register / List ---
assert(registry.has("task.claimed"), "builtin task.claimed registered");
assert(registry.list().length === Object.keys(BUILTIN_SCHEMAS).length, "all builtins listed");

let updateErr;
try { registry.register("task.claimed", {}); } catch (e) { updateErr = e; }
assert(updateErr && updateErr.message.includes("already registered"), "duplicate register throws");

// --- Valid event ---
const claimResult = registry.validate("task.claimed", {
  taskId: "t1", projectId: "p1", agentId: "a1", claimedAt: new Date().toISOString(),
});
assert(claimResult.valid === true, "valid task.claimed passes");
assert(claimResult.errors === null, "no errors on valid event");

// --- Missing required field ---
const badClaim = registry.validate("task.claimed", { taskId: "t1" });
assert(badClaim.valid === false, "missing fields fails validation");
assert(badClaim.errors.length > 0, "errors array populated");
assert(violations.length === 1, "violation emitted for badClaim");
assert(violations[0].schema === "schema-violation", "violation has correct schema tag");
assert(violations[0].data.eventType === "task.claimed", "violation captures eventType");

// --- Unknown event type ---
const unknownViolations = [];
const reg2 = createRegistry({ loadBuiltins: false });
reg2.onViolation((v) => unknownViolations.push(v));
const unk = reg2.validate("fake.event", { foo: 1 });
assert(unk.valid === false, "unknown type rejected");
assert(unknownViolations.length === 1, "violation emitted for unknown type");

// --- Allow unregistered ---
const reg3 = createRegistry({ loadBuiltins: false, allowUnregistered: true });
const unkAllowed = reg3.validate("any.event", { x: 42 });
assert(unkAllowed.valid === true, "unregistered allowed when option set");
assert(unkAllowed.unregistered === true, "flagged as unregistered");

// --- Custom schema registration ---
const reg4 = createRegistry({ loadBuiltins: false });
reg4.register("custom.event", {
  type: "object",
  required: ["id"],
  properties: { id: { type: "string" }, count: { type: "integer", minimum: 0 } },
  additionalProperties: false,
});

const customOk = reg4.validate("custom.event", { id: "abc", count: 5 });
assert(customOk.valid === true, "custom valid event passes");

const customBad = reg4.validate("custom.event", { id: "abc", count: -1 });
assert(customBad.valid === false, "negative count fails minimum");

const customExtra = reg4.validate("custom.event", { id: "abc", bogus: true });
assert(customExtra.valid === false, "additionalProperties rejected");

// --- Update schema ---
reg4.update("custom.event", {
  type: "object",
  required: ["id"],
  properties: { id: { type: "string" }, count: { type: "integer", minimum: 0 }, tag: { type: "string" } },
  additionalProperties: false,
});
const updatedOk = reg4.validate("custom.event", { id: "abc", count: 1, tag: "new" });
assert(updatedOk.valid === true, "updated schema accepts new field");

// --- Unregister ---
reg4.unregister("custom.event");
assert(reg4.has("custom.event") === false, "unregister removes schema");

// --- Unsubscribe listener ---
const reg5 = createRegistry({ loadBuiltins: false });
const spyViolations = [];
const unsub = reg5.onViolation((v) => spyViolations.push(v));
reg5.register("x.y", { type: "object", required: ["a"], properties: { a: { type: "string" } }, additionalProperties: false });
reg5.validate("x.y", {});
assert(spyViolations.length === 1, "listener received violation");
unsub();
reg5.validate("x.y", {});
assert(spyViolations.length === 1, "unsub stops notifications");

// --- Summary ---
console.log(`\nResults: ${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);