import json
import sqlite3
import time
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any


@dataclass
class Event:
    id: Optional[int] = None
    event_type: str = ""
    project_id: str = ""
    agent_id: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    created_at: float = 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["payload"] = json.dumps(d["payload"])
        return d

    @classmethod
    def from_row(cls, row: tuple) -> "Event":
        return cls(
            id=row[0],
            event_type=row[1],
            project_id=row[2],
            agent_id=row[3],
            payload=json.loads(row[4]),
            created_at=row[5],
        )


SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_project ON events(project_id);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
"""


class EventStore(ABC):
    @abstractmethod
    def append(self, event: Event) -> Event:
        ...

    @abstractmethod
    def replay(self, offset: int = 0, since: float = 0.0,
               event_type: Optional[str] = None,
               project_id: Optional[str] = None,
               limit: int = 1000) -> List[Event]:
        ...

    @abstractmethod
    def count(self) -> int:
        ...


class SQLiteEventStore(EventStore):
    def __init__(self, db_path: str = ":memory:"):
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = None
        self._conn.executescript(SCHEMA)

    def append(self, event: Event) -> Event:
        if event.created_at == 0.0:
            event.created_at = time.time()
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO events (event_type, project_id, agent_id, payload, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (event.event_type, event.project_id, event.agent_id,
                 json.dumps(event.payload), event.created_at),
            )
            event.id = cur.lastrowid
            self._conn.commit()
        return event

    def replay(self, offset: int = 0, since: float = 0.0,
               event_type: Optional[str] = None,
               project_id: Optional[str] = None,
               limit: int = 1000) -> List[Event]:
        clauses: List[str] = []
        params: List[Any] = []
        if offset > 0:
            clauses.append("id >= ?")
            params.append(offset)
        if since > 0.0:
            clauses.append("created_at >= ?")
            params.append(since)
        if event_type:
            clauses.append("event_type = ?")
            params.append(event_type)
        if project_id:
            clauses.append("project_id = ?")
            params.append(project_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        rows = self._conn.execute(
            f"SELECT id, event_type, project_id, agent_id, payload, created_at "
            f"FROM events{where} ORDER BY id ASC LIMIT ?",
            params,
        ).fetchall()
        return [Event.from_row(r) for r in rows]

    def count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM events").fetchone()
        return row[0]


class PostgresEventStore(EventStore):
    def __init__(self, dsn: str):
        import psycopg2
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = True
        cur = self._conn.cursor()
        cur.execute(SCHEMA)
        cur.close()

    def append(self, event: Event) -> Event:
        if event.created_at == 0.0:
            event.created_at = time.time()
        cur = self._conn.cursor()
        cur.execute(
            "INSERT INTO events (event_type, project_id, agent_id, payload, created_at) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (event.event_type, event.project_id, event.agent_id,
             json.dumps(event.payload), event.created_at),
        )
        event.id = cur.fetchone()[0]
        cur.close()
        return event

    def replay(self, offset: int = 0, since: float = 0.0,
               event_type: Optional[str] = None,
               project_id: Optional[str] = None,
               limit: int = 1000) -> List[Event]:
        clauses: List[str] = []
        params: List[Any] = []
        if offset > 0:
            clauses.append("id >= %s")
            params.append(offset)
        if since > 0.0:
            clauses.append("created_at >= %s")
            params.append(since)
        if event_type:
            clauses.append("event_type = %s")
            params.append(event_type)
        if project_id:
            clauses.append("project_id = %s")
            params.append(project_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur = self._conn.cursor()
        cur.execute(
            f"SELECT id, event_type, project_id, agent_id, payload, created_at "
            f"FROM events{where} ORDER BY id ASC LIMIT %s",
            params,
        )
        rows = cur.fetchall()
        cur.close()
        return [Event.from_row(r) for r in rows]

    def count(self) -> int:
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM events")
        val = cur.fetchone()[0]
        cur.close()
        return val


def create_store(db_url: Optional[str] = None) -> EventStore:
    if db_url and db_url.startswith("postgres"):
        return PostgresEventStore(db_url)
    path = db_url or ":memory:"
    return SQLiteEventStore(path)