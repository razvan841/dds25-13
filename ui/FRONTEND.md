# DDS UI Guide

This page explains how to build and run the demo frontend, and what each UI control does.

## Prerequisites
- Node.js 18+ and npm (or run via docker-compose).
- Backend stack up and reachable on http://localhost:8000 (gateway from compose).

## Run with npm (local dev)
1. Open a terminal in `dds25-13/ui`.
2. Install deps (first time): `npm install` *(uses package.json in this folder if present; otherwise plain static files need no install).* 
3. Start a static server (pick one):
   - With `npm`: `npm install -g serve` then `serve -l 8081 .`
   - Or Python: `python -m http.server 8081`.
4. Open `http://localhost:8081` in your browser.

## Run with docker-compose (recommended with the rest of the stack)
From repo root:
```
# 1) Start backend stack (gateway on localhost:8000)
docker compose up --build -d

# 2) Build UI image
docker build -t dds-ui ./ui

# 3) Run UI container (serves static files via nginx on localhost:8081)
docker run --rm -p 8081:80 --name dds-ui dds-ui

```
The UI will be served through the gateway at `http://localhost:8000`.

## Caching tip
If the UI seems unchanged after edits, force-reload (Ctrl+Shift+R) to bypass cache.

## Apply code changes to minikube
From repo root, rebuild and restart the backend stack with:
```bash
./redeploy-minikube.sh
```

Useful flags:
- `./redeploy-minikube.sh --no-ui` skips rebuilding the `dds-ui` image.
- `./redeploy-minikube.sh --no-wait` restarts deployments without waiting for rollouts to finish.

## UI walkthrough
- **Base URL control**
  - `Base URL`: lets you point the UI at a changing Minikube or gateway address, for example `http://$(minikube ip):<nodePort>`.
  - `Apply base URL`: saves the value in browser local storage and uses it for all subsequent API calls.
- **View switcher**
  - `Demo controls`: the existing user/item/order workflow.
  - `Operations dashboard`: a new mock observability page intended as the design target for future live monitoring.
- **Users card**
  - `Create user`: calls `/payment/create_user`, adds the new user to the list and selects it.
  - `Add funds`: uses `/payment/add_funds/{user}/{amount}` with the number in the field.
  - `Clear list`: clears the client-side list (no server calls).
  - Selecting a user fetches `/payment/find_user/{id}` and updates balance info.
- **User balance card**
  - `Refresh balance`: re-fetches `/payment/find_user/{selected}` and shows current credit.
- **Items card**
  - `Create item`: calls `/stock/item/create/{price}` using the price field, then caches the new item.
  - Per-item controls:
    - `Qty` + `Add to cart`: queues that quantity for the current order (client-side until sync).
    - `Remove from order (-qty)`: subtracts quantity from the cart entry.
    - `Stock Δ` + `Add to stock`: calls `/stock/add/{item}/{delta}` to increase inventory, then refreshes price/stock via `/stock/find/{item}`.
- **Inventory lookup card**
  - Paste one or more item IDs (comma/newline separated) and click `Fetch inventory`; for each ID it GETs `/stock/find/{id}` and displays stock/price or an inline error.
- **Order card**
  - `Create order`: POST `/orders/create/{user}` for the selected user.
  - `Sync cart → backend`: for each cart item, POST `/orders/addItem/{order}/{item}/{qty}` and refresh order state.
  - `Checkout`: POST `/orders/checkout/{order}` to start the saga and wait for a terminal result or timeout.
  - `Refresh order`: GET `/orders/find/{order}` and update the display.
  - Cart list: shows queued items and lets you remove them client-side.
- **Operations dashboard**
  - `Refresh dashboard`: GET `/monitoring/overview`.
  - Summary cards: top-level KPIs for instance health, saga backlog, active 2PL/2PC transactions, and database pressure.
  - `Service instances`: per-service, per-shard cards now read live instance data from service monitoring endpoints aggregated by the gateway.
  - `Database health`: Redis shard cards now read live reachability, ping latency, key count, and uptime values aggregated from the service monitoring endpoints.
  - `Saga status`: counts and recent example flows for `TRYING`, `RESERVED`, `COMMITTED`, and `FAILED`.
  - `2PL / 2PC status`: counts and recent example flows for `PREPARING`, `PREPARED`, `COMMITTING`, and `ABORTED`.

## Notes
- Gateway routes: `/payment/*`, `/stock/*`, `/orders/*` all go through the nginx gateway on port 8000.
- Monitoring routes:
  - `/monitoring/overview` on the gateway returns the dashboard snapshot.
  - `/monitoring/instance` on each service returns minimal live instance metadata used by the dashboard.
- Negative quantities in the cart UI will remove items from the order when synced.
- The UI stores minimal state locally; reloads may lose unsynced cart contents.
