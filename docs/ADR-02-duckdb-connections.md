# ADR-02: DuckDB with per-write connections instead of a database server

## Status
Accepted

## Context
The project needed a queryable datastore for the Streamlit dashboard to read
from. Two options were considered: Postgres (a full database server) and
DuckDB (an embedded, file-based database).

DuckDB was chosen for its simplicity — no server process to install, configure,
or manage, which matters for a project meant to be runnable by anyone who
clones the repo.

However, DuckDB is a single-writer, embedded database: only one process can
hold an open connection to the database file at a time. Initially, both
`consumer.py` (writer) and `dashboard.py` (reader) held long-lived connections
open for their entire runtime, which caused the dashboard to fail with
`IOException: File is already open in another process`.

## Decision
Instead of one long-lived connection per script, both `consumer.py` and
`dashboard.py` open a short-lived connection for each read/write operation
and close it immediately afterward. This keeps the exclusive lock held for
milliseconds rather than the entire program's runtime, allowing the dashboard
to read the file in the brief windows between the consumer's writes.

## Consequences
- **Positive:** No separate database server to install or manage — the
  entire pipeline runs from `docker compose up` plus three Python scripts.
- **Positive:** Fixed the concurrent read/write conflict without needing
  to switch to a client-server database.
- **Negative:** Opening a fresh connection per operation adds small overhead
  compared to a persistent connection. At this project's scale (a few
  messages per second), this overhead is negligible. A production system
  with much higher throughput would likely need Postgres or a proper
  client-server database instead.