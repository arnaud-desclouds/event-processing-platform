# Design Tradeoffs / Production Notes

This document lists the tradeoffs in this implementation. This is a local
event-processing stack, not a production-complete distributed system.

## Delivery Model

The worker assumes at-least-once delivery. A message can be seen more than once
because HTTP clients may retry, the broker can redeliver, and a
worker may fail after doing part of the work.

This project does not provide exactly-once processing. It handles duplicates at
the database boundary: persisted events are keyed by `event_id`, and repeated
writes are ignored.

In production, the delivery policy should match the business contract. Database
idempotency is enough for some workflows; others need an inbox/outbox table,
transactional producers, or reconciliation.

## Idempotency Strategy

PostgreSQL enforces idempotency with a unique `event_id` constraint and
`INSERT ... ON CONFLICT DO NOTHING`.

That keeps the source of truth in the same place as the stored event. It also
makes duplicate behavior easy to test: the API can accept repeated submissions,
the worker can process at least once, and PostgreSQL decides whether a row is
new.

This avoids Redis, an external idempotency service, and broker-level compaction.
The cost is that `event_id` must be globally meaningful for this project. In a
larger system, uniqueness might need to be scoped by tenant, source, event type,
or producer.

## DLQ Policy

Malformed messages and database write failures are sent to `events.dlq`.

The DLQ is for failures that need inspection, not for general retries. Entries
include the original payload and failure context. Replay is supported when the DLQ entry
contains a recoverable `failed_event`. Messages that were not valid events may
still require manual correction before they can be replayed.

Production replay would need operator approval, rate limits, audit logs, replay
batches, poison-message handling, and clear ownership for fixing malformed
producer payloads.

## Observability

The local stack includes Prometheus metrics, OpenTelemetry traces, readiness
checks, and Jaeger.

Metrics cover accepted events, processed events, deduped events, parsing
failures, persistence failures, and DLQ writes. Traces follow the HTTP request
through broker publishing, worker processing, and database persistence spans.

The local demo keeps `source` and `event_type` labels on event counters so the
README walkthrough can filter demo traffic. In production, those
producer-controlled labels should be allowlisted or removed.

This is enough for local debugging and behavior checks, but it is not a full
production observability setup. There are no alert rules, Grafana dashboards,
SLOs, log aggregation pipeline, or retention policies in this repository.

## Startup and Operability

The Compose stack starts topic bootstrap, migrations, the ingestion API, the
worker, Prometheus, the OpenTelemetry Collector, and Jaeger.

Readiness endpoints distinguish basic process liveness from usable dependencies:
the API checks broker topic metadata, and the worker checks that its PostgreSQL
pool and Kafka clients have started.

This is a local development setup. Production would need platform-native health
checks, secrets management, rolling deploys, backup and restore procedures,
capacity planning, and clearer failure ownership.

## Testing Boundaries

The test suite keeps three levels separate:

- unit and service tests cover validation, parsing, idempotent processing, DLQ
  behavior, and startup/shutdown paths
- Docker-backed end-to-end tests verify the API, broker, worker, and database
  together
- load testing is a local smoke check for the ingest path, not a capacity claim

Unit tests stay quick, and the Docker-backed tests check the real integration
path. The next production step would be scenario tests for broker outages,
database restarts, replay batches, and migration compatibility.

## Production Readiness Gaps

These expand the shorter Known Limits section in the README:

- event contract management: schema versioning, compatibility checks, and a
  clear rollout path for producer changes
- operational controls: alerting on processing failures, queue lag, DLQ growth,
  and replay activity
- failure handling: bounded retries, backpressure, poison-message handling, and
  safer replay batches
- infrastructure hardening: managed broker/database, secrets management,
  backups, and restore testing
