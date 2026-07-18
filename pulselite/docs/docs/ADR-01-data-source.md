# ADR-01: Simulated data instead of live Reddit API

## Status
Accepted

## Context
The original plan was to pull live posts from Reddit's public `.json` endpoint
(e.g. `reddit.com/r/india/new.json`) as the data source for this pipeline.

During development, this endpoint consistently returned `403 Client Error: Blocked`,
even after:
- Adding a descriptive User-Agent header per Reddit's guidelines
- Switching to a browser-style User-Agent
- Registering an official Reddit API "script" app

Reddit's current developer platform requires manual application review for
broader API access, which is not compatible with a short project timeline.

## Decision
Use a local simulated data generator (`producer_fetch.py`) that produces posts
matching the exact schema a real Reddit post would have: id, title, author,
score, num_comments, created_utc, subreddit, url, and embedded hashtags.

## Consequences
- **Positive:** Development was not blocked by an external service's
  availability or approval process. The generator produces varied,
  realistic sentiment (positive/negative/neutral) and hashtag entities,
  giving the sentiment and entity-extraction stages meaningful data to work with.
- **Positive:** Because the schema matches Reddit's real post structure,
  a real API (Reddit's official OAuth API, or Twitter/X) can be swapped in
  later by only changing `producer_fetch.py` — Kafka, the consumer, the
  database schema, and the dashboard all remain unchanged.
- **Negative:** The data is not truly reflective of real-world traffic
  patterns (spam, coordinated posting, real bursty events), which is
  something a production system would need to account for.