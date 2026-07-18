# ADR-03: VADER instead of a transformer model for sentiment

## Status
Accepted

## Context
Sentiment scoring could be done with a lightweight rule-based tool (VADER)
or a transformer-based model (e.g. a fine-tuned BERT/RoBERTa sentiment
classifier via HuggingFace).

## Decision
Use VADER (Valence Aware Dictionary and sEntiment Reasoner) for sentiment
scoring in the streaming consumer.

## Consequences
- **Positive:** VADER is extremely fast (no model loading, no GPU, no
  inference latency) — critical for a streaming consumer that needs to
  keep up with incoming Kafka messages in real time.
- **Positive:** VADER is specifically tuned for short, informal social-media
  text (handles punctuation emphasis, capitalization, and common slang
  reasonably well), which matches this project's data.
- **Positive:** Zero external dependencies at inference time — no model
  downloads, no API calls, fully deterministic and offline.
- **Negative:** VADER cannot capture context, sarcasm, or domain-specific
  nuance the way a fine-tuned transformer model could. For a production
  system prioritizing accuracy over latency, a transformer-based sentiment
  model would likely outperform VADER, at the cost of higher compute and
  latency per message.
- **Future work:** A transformer model could be introduced as an optional
  second processing path, run asynchronously so it doesn't block the
  real-time pipeline.