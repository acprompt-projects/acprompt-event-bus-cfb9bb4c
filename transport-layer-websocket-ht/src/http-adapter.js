const http = require('http');
const EventEmitter = require('events');

class HttpAdapter extends EventEmitter {
  constructor(eventBus, opts = {}) {
    super();
    this.bus = eventBus;
    this.port = opts.port || 9201;
    this.server = null;
    this._buffer = [];
    this._maxBuffer = opts.maxBuffer || 1000;
    this._corsOrigin = opts.corsOrigin || '*';
  }

  start() {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => this._handle(req, res));
      this.server.on('error', reject);
      this.server.listen(this.port, () => {
        this.server.removeListener('error', reject);
        this.bus.on('event', (channel, payload) => {
          this._buffer.push({channel, payload, ts: Date.now()});
          if (this._buffer.length > this._maxBuffer) this._buffer.shift();
        });
        resolve(this.server);
      });
    });
  }

  stop() {
    return new Promise((resolve, reject) => {
      if (!this.server) return resolve();
      this.server.close(err => err ? reject(err) : resolve());
    });
  }

  _cors(res) {
    res.setHeader('Access-Control-Allow-Origin', this._corsOrigin);
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  }

  _json(res, status, body) {
    this._cors(res);
    res.writeHead(status, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(body));
  }

  _handle(req, res) {
    if (req.method === 'OPTIONS') { this._cors(res); res.writeHead(204); res.end(); return; }

    const url = new URL(req.url, `http://${req.headers.host}`);
    const parts = url.pathname.replace(/\/+$/, '').split('/');

    // POST /events/:channel  — publish an event
    if (req.method === 'POST' && parts.length === 3 && parts[1] === 'events') {
      const channel = parts[2];
      let body = '';
      req.on('data', c => { body += c; if (body.length > 1e6) req.destroy(); });
      req.on('end', () => {
        try {
          const payload = JSON.parse(body);
          this.bus.emit('event', channel, payload);
          const entry = {channel, payload, ts: Date.now()};
          this._buffer.push(entry);
          if (this._buffer.length > this._maxBuffer) this._buffer.shift();
          this._json(res, 201, {ok: true, ...entry});
        } catch { this._json(res, 400, {error: 'invalid JSON'}); }
      });
      return;
    }

    // GET /events — list recent events (with optional ?channel= filter & ?since= ts)
    if (req.method === 'GET' && parts[1] === 'events' && !parts[2]) {
      const ch = url.searchParams.get('channel');
      const since = parseInt(url.searchParams.get('since'), 10) || 0;
      let results = this._buffer;
      if (ch) results = results.filter(e => e.channel === ch);
      if (since) results = results.filter(e => e.ts > since);
      this._json(res, 200, {events: results});
      return;
    }

    // GET /events/:channel — list recent events for a specific channel
    if (req.method === 'GET' && parts.length === 3 && parts[1] === 'events') {
      const channel = parts[2];
      const since = parseInt(url.searchParams.get('since'), 10) || 0;
      let results = this._buffer.filter(e => e.channel === channel);
      if (since) results = results.filter(e => e.ts > since);
      this._json(res, 200, {events: results});
      return;
    }

    // GET /health
    if (req.method === 'GET' && url.pathname === '/health') {
      this._json(res, 200, {status: 'ok', buffered: this._buffer.length});
      return;
    }

    this._json(res, 404, {error: 'not found'});
  }
}

module.exports = {HttpAdapter};