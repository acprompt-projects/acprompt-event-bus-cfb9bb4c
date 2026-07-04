===
const { EventBusServer } = require("../../src/server");
const WebSocket = require("ws");
const http = require("http");

const TEST_PORT = 9876;
const BASE_URL = `http://localhost:${TEST_PORT}`;
const WS_URL = `ws://localhost:${TEST_PORT}/ws`;

let server, httpServer;

function request(method, path, body) {
  return new Promise((resolve, reject) => {
    const opts = {
      hostname: "localhost",
      port: TEST_PORT,
      path,
      method,
      headers: { "Content-Type": "application/json" },
    };
    const req = http.request(opts, (res) => {
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        const parsed = data ? JSON.parse(data) : {};
        resolve({ status: res.statusCode, body: parsed });
      });
    });
    req.on("error", reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

function wsConnect(subscription) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL);
    const received = [];
    ws.on("open", () => {
      if (subscription) ws.send(JSON.stringify({ type: "subscribe", ...subscription }));
      resolve({ ws, received, close: () => ws.close() });
    });
    ws.on("message", (raw) => received.push(JSON.parse(raw.toString())));
    ws.on("error", reject);
  });
}

function waitForEvents(conn, count, timeout = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timeout: got ${conn.received.length}/${count}`)), timeout);
    const check = () => {
      if (conn.received.length >= count) {
        clearTimeout(timer);
        resolve(conn.received);
      }
    };
    conn.ws.on("message", check);
    check();
  });
}

beforeAll(async () => {
  httpServer = http.createServer();
  server = new EventBusServer(httpServer);
  await new Promise((r) => httpServer.listen(TEST_PORT, r));
});

afterAll(async () => {
  await new Promise((r) => httpServer.close(r));
});

afterEach(async () => {
  server.reset();
});

describe("REST publish and subscribe flow", () => {
  test("publish event and retrieve via REST", async () => {
    const event = {
      type: "task.claimed",
      project: "proj-1",
      agent: "agent-alpha",
      payload: { taskId: "t-101" },
    };
    const pub = await request("POST", "/events", event);
    expect(pub.status).toBe(201);
    expect(pub.body.id).toBeDefined();

    const sub = await request("GET", `/events?project=proj-1&type=task.claimed`);
    expect(sub.status).toBe(200);
    expect(sub.body.events).toHaveLength(1);
    expect(sub.body.events[0].type).toBe("task.claimed");
    expect(sub.body.events[0].agent).toBe("agent-alpha");
  });

  test("subscribe filters by project only", async () => {
    await request("POST", "/events", { type: "task.completed", project: "proj-a", agent: "a1", payload: {} });
    await request("POST", "/events", { type: "note.posted", project: "proj-b", agent: "a2", payload: {} });
    await request("POST", "/events", { type: "task.completed", project: "proj-a", agent: "a3", payload: {} });

    const res = await request("GET", `/events?project=proj-a`);
    expect(res.body.events).toHaveLength(2);
    expect(res.body.events.every((e) => e.project === "proj-a")).toBe(true);
  });

  test("with empty result returns empty array", async () => {
    const res = await request("GET", `/events?project=noexist`);
    expect(res.status).toBe(200);
    expect(res.body.events).toEqual([]);
  });
});

describe("WebSocket pub/sub flow", () => {
  test("ws subscriber receives published events in real time", async () => {
    const conn = await wsConnect({ project: "proj-ws" });
    await request("POST", "/events", { type: "task.claimed", project: "proj-ws", agent: "a1", payload: { taskId: "t-1" } });

    const events = await waitForEvents(conn, 1);
    expect(events[0].type).toBe("task.claimed");
    expect(events[0].payload.taskId).toBe("t-1");
    conn.close();
  });

  test("ws subscriber only receives events for subscribed project", async () => {
    const conn = await wsConnect({ project: "proj-x" });
    await request("POST", "/events", { type: "note.posted", project: "proj-y", agent: "a1", payload: {} });
    await request("POST", "/events", { type: "task.completed", project: "proj-x", agent: "a2", payload: {} });

    await waitForEvents(conn, 1);
    expect(conn.received).toHaveLength(1);
    expect(conn.received[0].project).toBe("proj-x");
    conn.close();
  });

  test("ws wildcard subscription receives all events", async () => {
    const conn = await wsConnect({ project: "*" });
    await request("POST", "/events", { type: "invite.sent", project: "any-proj", agent: "a1", payload: {} });

    const events = await waitForEvents(conn, 1);
    expect(events[0].type).toBe("invite.sent");
    conn.close();
  });
});

describe("Cross-transport scenarios", () => {
  test("event published via REST is visible to both REST and WS consumers", async () => {
    const wsConn = await wsConnect({ project: "proj-cross" });
    await request("POST", "/events", { type: "task.claimed", project: "proj-cross", agent: "a1", payload: { tid: "c1" } });

    await waitForEvents(wsConn, 1);

    const restRes = await request("GET", `/events?project=proj-cross`);
    expect(restRes.body.events).toHaveLength(1);

    expect(wsConn.received[0].id).toBe(restRes.body.events[0].id);
    wsConn.close();
  });

  test("multiple ws subscribers all receive the same event", async () => {
    const conn1 = await wsConnect({ project: "proj-multi" });
    const conn2 = await wsConnect({ project: "proj-multi" });

    await request("POST", "/events", { type: "note.posted", project: "proj-multi", agent: "a1", payload: { text: "hi" } });

    const [ev1] = await waitForEvents(conn1, 1);
    const [ev2] = await waitForEvents(conn2, 1);
    expect(ev1.id).toBe(ev2.id);
    expect(ev1.type).toBe("note.posted");

    conn1.close();
    conn2.close();
  });
});

describe("Schema validation rejection", () => {
  test("rejects event with missing required fields", async () => {
    const res = await request("POST", "/events", { type: "task.claimed" });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/missing/i);
  });

  test("rejects event with invalid type", async () => {
    const res = await request("POST", "/events", {
      type: "invalid.event",
      project: "p1",
      agent: "a1",
      payload: {},
    });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/invalid.*type/i);
  });

  test("rejects event with non-object payload", async () => {
    const res = await request("POST", "/events", {
      type: "task.claimed",
      project: "p1",
      agent: "a1",
      payload: "bad",
    });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/payload/i);
  });

  test("rejects event with missing agent", async () => {
    const res = await request("POST", "/events", {
      type: "task.completed",
      project: "p1",
      payload: {},
    });
    expect(res.status).toBe(400);
  });
});

describe("Replay correctness", () => {
  test("replays events from a given sequence number", async () => {
    await request("POST", "/events", { type: "task.claimed", project: "proj-rep", agent: "a1", payload: { n: 1 } });
    await request("POST", "/events", { type: "task.completed", project: "proj-rep", agent: "a1", payload: { n: 2 } });
    await request("POST", "/events", { type: "note.posted", project: "proj-rep", agent: "a2", payload: { n: 3 } });

    const allRes = await request("GET", `/events?project=proj-rep`);
    const allEvents = allRes.body.events;
    expect(allEvents).toHaveLength(3);

    const seqAfterFirst = allEvents[0].seq;
    const replayRes = await request("GET", `/events?project=proj-rep&since=${seqAfterFirst}`);
    const replayed = replayRes.body.events;
    expect(replayed).toHaveLength(2);
    expect(replayed[0].type).toBe("task.completed");
    expect(replayed[1].type).toBe("note.posted");
  });

  test("replay returns empty if since is beyond current seq", async () => {
    await request("POST", "/events", { type: "invite.sent", project: "proj-rep2", agent: "a1", payload: {} });
    const res = await request("GET", `/events?project=proj-rep2&since=999999`);
    expect(res.body.events).toEqual([]);
  });

  test("ws client can request replay on connect", async () => {
    await request("POST", "/events", { type: "task.claimed", project: "proj-wsrep", agent: "a1", payload: {} });

    const conn = await wsConnect({ project: "proj-wsrep", replayFrom: 0 });
    await waitForEvents(conn, 1);
    expect(conn.received[0].type).toBe("task.claimed");
    conn.close();
  });

  test("replay preserves ordering", async () => {
    for (let i = 0; i < 5; i++) {
      await request("POST", "/events", { type: "task.claimed", project: "proj-order", agent: "a1", payload: { i } });
    }
    const res = await request("GET", `/events?project=proj-order&since=0`);
    const events = res.body.events;
    for (let i = 1; i < events.length; i++) {
      expect(events[i].seq).toBeGreaterThan(events[i - 1].seq);
    }
  });
});
===