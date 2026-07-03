import json
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime

from event_store import Event, create_store

store = create_store(os.getenv("DATABASE_URL"))


class ReplayHandler(BaseHTTPRequestHandler):
    def _json(self, code: int, data: object):
        body = json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/events/replay":
            qs = parse_qs(parsed.query)
            offset = int(qs.get("offset", ["0"])[0])
            limit = min(int(qs.get("limit", ["1000"])[0]), 10000)
            event_type = qs.get("event_type", [None])[0]
            project_id = qs.get("project_id", [None])[0]
            since_str = qs.get("since", [None])[0]
            since = 0.0
            if since_str:
                try:
                    since = datetime.fromisoformat(since_str).timestamp()
                except ValueError:
                    since = float(since_str)
            events = store.replay(offset=offset, since=since,
                                  event_type=event_type, project_id=project_id,
                                  limit=limit)
            self._json(200, {
                "events": [e.to_dict() for e in events],
                "count": len(events),
                "offset": offset,
            })
        elif parsed.path == "/events/count":
            self._json(200, {"count": store.count()})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/events":
            try:
                data = json.loads(self._read_body())
            except (json.JSONDecodeError, ValueError) as exc:
                self._json(400, {"error": f"invalid json: {exc}"})
                return
            event = Event(
                event_type=data.get("event_type", ""),
                project_id=data.get("project_id", ""),
                agent_id=data.get("agent_id", ""),
                payload=data.get("payload", {}),
                created_at=data.get("created_at", 0.0),
            )
            if not event.event_type:
                self._json(400, {"error": "event_type is required"})
                return
            saved = store.append(event)
            self._json(201, saved.to_dict())
        else:
            self._json(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[replay-store] {args[0]}\n")


def main():
    port = int(os.getenv("PORT", "8090"))
    server = HTTPServer(("0.0.0.0", port), ReplayHandler)
    print(f"Replay store listening on :{port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()