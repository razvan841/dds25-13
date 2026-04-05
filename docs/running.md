# Running the Project

## Starting Services

```bash
# Start all services (builds images + runs containers)
docker-compose up --build

# Run in background
docker-compose up --build -d

# Tear down
docker-compose down
```

The gateway is exposed on **port 8000**.

## Checkout Modes

The system supports two checkout modes, controlled by the `CHECKOUT_MODE` environment variable on the order service:

- **`saga`** (default) — Orchestrated saga with compensating transactions. Stock items are subtracted sequentially, then payment is processed. On failure, compensating `stock_add` commands roll back.
- **`2pc`** — Two-Phase Locking + Two-Phase Commit. Locks are acquired during PREPARE (no data modified), data is modified during COMMIT, and locks are released during ABORT. Retries on lock contention with exponential backoff (max 5 retries).

### Switching Modes

Edit `env/checkout.env` and set the desired mode:

```env
CHECKOUT_MODE=2pc
```

Then rebuild:

```bash
docker-compose up --build -d
```

## Running Tests

Tests require services to be running:

```bash
docker-compose up --build -d
cd test
python -m unittest test_microservices.py
```

## Running the Consistency Test

```bash
docker-compose up --build -d
cd ../wdm-project-benchmark/consistency-test
python3 run_consistency_test.py
```
