# ADR-04: Resolving DuckDB lock contention between consumer and dashboard

## Status
Accepted

## Context
After containerizing all services (see Day 11 / Docker Compose setup), the
consumer began crash-looping with `IOException: Could not set lock on file`
errors. Initial attempts to fix this included switching from a Windows bind
mount to a Docker named volume, and reordering container startup — both
helped but did not fully resolve the issue.

Root cause: the dashboard opened a DuckDB read-only connection on every
5-second Streamlit rerun but never explicitly closed the previous one,
causing open connections to accumulate and eventually hold the file lock
indefinitely against the consumer's writes.

## Decision
Two changes together resolved this permanently:
1. Consumer writes are wrapped in a retry-with-backoff helper
   (`run_with_retry`), so a momentary lock conflict is retried instead of
   crashing the whole process.
2. The dashboard explicitly closes its DuckDB connection (`con.close()`)
   at the end of every script rerun, ensuring no connections accumulate.

## Consequences
- **Positive:** The system is now resilient to normal, expected lock
  contention between a single writer and a polling reader - retrying
  briefly is the correct pattern rather than failing hard.
- **Positive:** This mirrors a real production lesson: with an embedded,
  single-writer database, connection lifecycle management matters as much
  as the locking model itself.
- **Negative:** This required real debugging across multiple failed
  attempts (bind mount vs named volume, startup ordering, retry logic)
  before finding the actual root cause - a reminder that first fixes for
  concurrency bugs are often incomplete.
