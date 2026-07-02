const {WebSocketServer, WebSocket} = require('ws');

const HEARTBEAT_INTERVAL_MS = 15_000;
const HEARTBEAT_TIMEOUT_MS = 5_000;
const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 30_000;

/* ── Server-side adapter ─────────────────────────────────────────────── */

class WebSocketServerAdapter {
  constructor(eventBus, opts = {}) {
    this.bus = eventBus;
    this.port = opts.port || 9200;
    this.path = opts.path || '/ws';
    this.heartbeatInterval = opts.heartbeatInterval || HEARTBEAT_INTERVAL_MS;
    this.heartbeatTimeout = opts.heartbeatTimeout || HEARTBEAT_TIMEOUT_MS;
    this.wss = null;
    this._timers = new Map();
  }

  start() {
    return new Promise((resolve, reject) => {
      this.wss = new WebSocketServer({port: this.port, path: this.path});
      this.wss.on('error', reject);
      this.wss.on('listening', () => {
        this.wss.removeListener('error', reject);
        this._installHandlers();
        resolve(this.wss);
      });
    });
  }

  stop() {
    if (!this.wss) return Promise.resolve();
    for (const t of this._timers.values()) clearInterval(t);
    this._timers.clear();
    return new Promise((resolve, reject) => {
      this.wss.close(err => err ? reject(err) : resolve());
    });
  }

  _installHandlers() {
    this.wss.on('connection', (ws, req) => {
      ws.isAlive = true;
      ws.on('pong', () => { ws.isAlive = true; });

      ws.on('message', raw => {
        try {
          const msg = JSON.parse(raw);
          if (msg.type === 'subscribe') {
            ws.channels = ws.channels || new Set();
            ws.channels.add(msg.channel);
            return;
          }
          if (msg.type === 'unsubscribe') {
            ws.channels?.delete(msg.channel);
            return;
          }
          if (msg.type === 'publish') {
            this.bus.emit('event', msg.channel, msg.payload);
            this._broadcast(msg.channel, msg.payload, ws);
            return;
          }
        } catch { /* ignore malformed */ }
      });

      const hb = setInterval(() => {
        if (!ws.isAlive) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
      }, this.heartbeatInterval);
      this._timers.set(ws, hb);

      ws.on('close', () => {
        clearInterval(hb);
        this._timers.delete(ws);
      });
    });
  }

  _broadcast(channel, payload, origin) {
    const data = JSON.stringify({type: 'event', channel, payload, ts: Date.now()});
    for (const ws of this.wss.clients) {
      if (ws === origin || ws.readyState !== WebSocket.OPEN) continue;
      if (ws.channels && !ws.channels.has(channel)) continue;
      ws.send(data);
    }
  }

  broadcast(channel, payload) {
    this._broadcast(channel, payload, null);
  }
}

/* ── Client-side adapter ─────────────────────────────────────────────── */

class WebSocketClientAdapter {
  constructor(eventBus, opts = {}) {
    this.bus = eventBus;
    this.url = opts.url || 'ws://localhost:9200/ws';
    this.reconnectBase = opts.reconnectBase || RECONNECT_BASE_MS;
    this.reconnectMax = opts.reconnectMax || RECONNECT_MAX_MS;
    this.heartbeatInterval = opts.heartbeatInterval || HEARTBEAT_INTERVAL_MS;
    this.heartbeatTimeout = opts.heartbeatTimeout || HEARTBEAT_TIMEOUT_MS;
    this.ws = null;
    this._attempts = 0;
    this._closed = false;
    this._hbTimer = null;
    this._missed = 0;
    this.subscriptions = new Set();
  }

  connect() {
    this._closed = false;
    return new Promise((resolve, reject) => {
      const onErr = (err) => { reject(err); };
      const ws = new WebSocket(this.url);
      ws.once('error', onErr);
      ws.once('open', () => {
        ws.removeListener('error', onErr);
        this.ws = ws;
        this._attempts = 0;
        this._startHeartbeat();
        this._resubscribe();
        ws.on('message', raw => this._onMessage(raw));
        ws.on('close', () => this._onClose());
        resolve(ws);
      });
    });
  }

  disconnect() {
    this._closed = true;
    clearInterval(this._hbTimer);
    if (this.ws) this.ws.close();
    this.ws = null;
  }

  subscribe(channel) {
    this.subscriptions.add(channel);
    this._send({type: 'subscribe', channel});
  }

  unsubscribe(channel) {
    this.subscriptions.delete(channel);
    this._send({type: 'unsubscribe', channel});
  }

  publish(channel, payload) {
    this._send({type: 'publish', channel, payload});
  }

  _send(msg) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  _resubscribe() {
    for (const ch of this.subscriptions) this._send({type: 'subscribe', channel: ch});
  }

  _onMessage(raw) {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'event') this.bus.emit('event', msg.channel, msg.payload);
      if (msg.type === 'pong') { this._missed = 0; }
    } catch { /* ignore */ }
  }

  _startHeartbeat() {
    clearInterval(this._hbTimer);
    this._missed = 0;
    this._hbTimer = setInterval(() => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      this._missed++;
      if (this._missed > Math.ceil(this.heartbeatTimeout / this.heartbeatInterval)) {
        this.ws.terminate();
        return;
      }
      this._send({type: 'ping'});
    }, this.heartbeatInterval);
  }

  _onClose() {
    clearInterval(this._hbTimer);
    this.ws = null;
    if (this._closed) return;
    const delay = Math.min(this.reconnectBase * 2 ** this._attempts, this.reconnectMax);
    this._attempts++;
    setTimeout(() => { if (!this._closed) this.connect().catch(() => {}); }, delay);
  }
}

module.exports = {WebSocketServerAdapter, WebSocketClientAdapter};