# Order Service – Kafka Saga Integration Notes



## High-level flow
1) HTTP client calls `POST /checkout/<order_id>`.
2) Order enqueues two Kafka commands (ReserveFunds/ReserveStock) into its Redis outbox.
3) Background worker (`reaper_worker.py`) drains the outbox and publishes to Kafka topics using `order_id` as the key.
4) Payment/Stock (once wired) consume commands, emit events.
5) Order's consumer handles events, updates saga state in Redis, and issues follow-up commands (commit/cancel) via the outbox.
6) When both commits arrive, the order is marked paid. Clients poll `GET /checkout_status/<order_id>` for status.
7) A deadline reaper cancels any saga stuck past its deadline.

## Topics and message keys
- Commands: `payment.commands`, `stock.commands`
- Events: `payment.events`, `stock.events`
- Optional: `order.events` (not emitted yet)
- **Key:** always `order_id` (transaction_id) to keep per-order ordering.

## Components (order service)
- `kafka_models.py`: Envelope + command/event payload structs; topic/group constants.
- `kafka_codec.py`: JSON (msgspec) encode/decode with validation.
- `kafka_config.py`: loads env vars for Kafka (bootstrap, client_id, group_id, optional SASL/TLS).
- `kafka_producer.py`: lazy singleton producer; `publish_envelope(topic, key, envelope)`.
- `kafka_consumer.py`: manual-commit consumer scaffold; used by the worker.
- `common_kafka/saga/outbox.py`: Redis saga state (status, reservation ids, commit flags, deadline), idempotency set, explicit-topic outbox, iterator helpers.
- `app.py`: HTTP endpoints and event handler (no background threads here). Checkout creates saga + outbox entries and returns 202. `/checkout_status` reads saga status. Logging forced to stdout.
- `reaper_worker.py`: dedicated process that runs:
  - Kafka consumer (uses `_handle_event` from `app.py`)
  - Outbox publisher loop (pops explicit-topic entries and publishes)
  - Deadline reaper (enqueues cancels on timeout)
  Logging forced to stdout with `[order-worker]` prefix.

## Runtime processes
- Web: `gunicorn` runs `order.app`. No background saga threads here.
- Worker: run `python -m order.reaper_worker` (or set command to that in compose/K8s). Starts consumer/outbox/reaper threads.

## Environment (order)
- `KAFKA_BOOTSTRAP` (e.g., `kafka-1:9092,kafka-2:9092,kafka-3:9092`)
- `KAFKA_CLIENT_ID` (default `order-service`)
- `KAFKA_GROUP_ID` (default `order-orchestrator`)
- `CHECKOUT_DEADLINE_SECONDS` (default 30)
- Redis vars already present (`REDIS_HOST/PORT/PASSWORD/DB`).
- Logging goes to stdout by default.

## Endpoints
- `POST /checkout/<order_id>` → returns `{"order_id", "status": "TRYING"}` after enqueueing Kafka commands.
- `GET /checkout_status/<order_id>` → returns current saga status (`TRYING|RESERVED|COMMITTED|CANCELLED|FAILED`).

## Saga state (Redis keys)
- `saga:<order_id>` hash: status, correlation_id, deadline_ts, payment_reservation_id, stock_reservation_id, funds_committed, stock_committed.
- `saga:<order_id>:processed` set: processed Kafka message_ids (idempotency).
- `saga:<order_id>:outbox` list: msgpack entries `{topic, payload}` (payload is encoded Envelope).

## Message schema (Envelope)
```json
{
  "type": "ReserveFundsCommand",   // or event type
  "version": 1,
  "message_id": "uuid",
  "saga_id": "order_id",
  "correlation_id": "uuid-per-checkout",
  "causation_id": "prev-message-id or null",
  "timestamp": "2026-02-19T12:34:56Z",
  "payload": { ... }
}
```
Payload structs are in `kafka_models.py` (funds/stock reserve, commit, cancel; corresponding events).

## How to wire Payment/Stock similarly
1) Add Kafka client deps (kafka-python) and env (`KAFKA_BOOTSTRAP`, client_id, group_id).
2) Reuse `kafka_models.py` or mirror its types to stay in sync.
3) Commands consumed:
   - Payment: `payment.commands` -> ReserveFundsCommand / CommitFundsCommand / CancelFundsCommand
   - Stock: `stock.commands` -> ReserveStockCommand / CommitStockCommand / CancelStockCommand
4) Events produced:
   - Payment -> `payment.events`: FundsReservedEvent / FundsReserveFailedEvent / FundsCommittedEvent / FundsCancelledEvent
   - Stock   -> `stock.events`: StockReservedEvent / StockReserveFailedEvent / StockCommittedEvent / StockCancelledEvent
5) Use `order_id` as Kafka key on all produces.
6) Implement idempotency (track processed message_ids) and local outbox for crash-consistent publish.
7) Commit offsets only after local state is written.
8) Optional: emit `order.events` for high-level status if desired.

## Startup / testing checklist
- Ensure `env/kafka.env` includes all topics; `init-topics.sh` creates them on startup.
- Run order web: `docker compose up order-service` (or the full stack).
- Run worker: add a service or process with command `python -m order.reaper_worker`.
- Hit `/checkout/<order_id>` then poll `/checkout_status/<order_id>`.
- Watch logs: `[order]` for web, `[order-worker]` for worker; Kafka consumer/producer logs appear in worker.

## Known gaps / next steps
- Payment/Stock are still REST-based; need Kafka wiring as above.
- No `order.events` emission yet.
- Integration tests for Kafka saga flow are pending.
```
