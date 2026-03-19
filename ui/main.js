// UI logic for DDS demo and proposed operations dashboard.
const API_BASE_QUERY_PARAM = "api";
const API_BASE_STORAGE_KEY = "dds-ui-api-base";
const ACTIVE_VIEW_STORAGE_KEY = "dds-ui-active-view";

const DEFAULT_API_BASE = (() => {
  const host = window.location.hostname;
  if (host === "gateway") return "http://gateway:80";
  if (host === "localhost" || host === "127.0.0.1" || host === "") return "http://localhost:8000";
  return `${window.location.protocol}//${host}:8000`;
})();

let userId = null;
let orderId = null;
let selectedUserId = null;
const users = [];
const cart = [];
const itemCache = new Map();
let inventoryResults = [];
let opsSnapshot = null;
let apiBase = loadApiBase();
let activeView = loadActiveView();

function normalizeApiBase(value) {
  return String(value || "").trim().replace(/\/$/, "");
}

function loadApiBase() {
  const qs = new URLSearchParams(window.location.search);
  const override = normalizeApiBase(qs.get(API_BASE_QUERY_PARAM));
  if (override) return override;
  const saved = window.localStorage.getItem(API_BASE_STORAGE_KEY);
  return normalizeApiBase(saved || DEFAULT_API_BASE);
}

function loadActiveView() {
  const saved = window.localStorage.getItem(ACTIVE_VIEW_STORAGE_KEY);
  return saved === "ops" ? "ops" : "demo";
}

function getApiBase() {
  return apiBase;
}

async function request(method, path) {
  const res = await fetch(`${getApiBase()}${path}`, { method });
  if (!res.ok) throw new Error(await res.text());
  const ct = res.headers.get("content-type") || "";
  return ct.includes("application/json") ? res.json() : res.text();
}

const get = (p) => request("GET", p);
const post = (p) => request("POST", p);

function setInfo(id, text, muted = false) {
  const el = document.getElementById(id);
  el.textContent = text;
  el.classList.toggle("muted", muted);
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(value);
}

function clampPercent(value) {
  return Math.max(0, Math.min(100, Number(value) || 0));
}

function formatDuration(seconds) {
  if (seconds == null) return "n/a";
  const total = Number(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}

function statusClass(status) {
  const value = String(status || "").toLowerCase();
  if (["healthy", "running", "committed", "active"].includes(value)) return "ok";
  if (["degraded", "warning", "preparing", "prepared", "retrying", "cancelled", "reserved", "trying"].includes(value)) return "warn";
  return "fail";
}

function renderUserList() {
  const list = document.getElementById("userList");
  if (!users.length) {
    list.innerHTML = '<div class="muted">No users yet</div>';
    return;
  }
  list.innerHTML = users.map((id) => `
    <div class="item-row ${id === selectedUserId ? "selected" : ""}">
      <div><code>${id}</code>${id === selectedUserId ? " (selected)" : ""}</div>
      <button data-select="${id}">${id === selectedUserId ? "Using" : "Use this"}</button>
    </div>
  `).join("");
  list.querySelectorAll("button[data-select]").forEach((btn) => {
    btn.onclick = wrap(() => selectUser(btn.dataset.select));
  });
}

async function selectUser(id) {
  selectedUserId = id;
  userId = id;
  renderUserList();
  try {
    const u = await get(`/payment/find_user/${id}`);
    setInfo("userInfo", `User: ${u.user_id} | credit ${u.credit}`, false);
    setInfo("balanceInfo", `Balance: ${u.credit}`, false);
  } catch (err) {
    setInfo("userInfo", `User: ${id} (credit unavailable)`, true);
    setInfo("balanceInfo", "Balance unavailable", true);
    throw err;
  }
}

async function addUser(id) {
  if (!users.includes(id)) users.unshift(id);
  await selectUser(id);
}

function clearUsers() {
  users.length = 0;
  selectedUserId = null;
  userId = null;
  renderUserList();
  setInfo("userInfo", "No user yet", true);
}

function formatOrder(o) {
  return `Order: ${o.order_id} | paid: ${o.paid} | total: $${o.total_cost} | items: ${o.items.length}`;
}

function renderItems() {
  const list = document.getElementById("itemList");
  if (itemCache.size === 0) {
    list.innerHTML = '<div class="muted">No items yet</div>';
    return;
  }
  list.innerHTML = [...itemCache.entries()].map(([id, meta]) => {
    const price = meta.price ?? "?";
    const stock = meta.stock ?? "?";
    return `
      <div class="item-row">
        <div><code>${id}</code> - $${price} - stock: ${stock}</div>
        <label>Qty <input type="number" data-id="${id}" value="1" step="1"></label>
        <button data-add="${id}">Add to cart</button>
        <button data-add="${id}" data-negative="1">Remove from order (-qty)</button>
        <label class="inline">Stock D <input type="number" data-stock-input="${id}" value="5" step="1" min="1"></label>
        <button data-add-stock="${id}">Add to stock</button>
      </div>
    `;
  }).join("");

  list.querySelectorAll("button[data-add]").forEach((btn) => {
    btn.onclick = () => {
      const id = btn.dataset.add;
      const input = list.querySelector(`input[data-id="${id}"]`);
      let qty = Number(input.value || 0);
      if (btn.dataset.negative) qty = -Math.abs(qty || 1);
      addToCart(id, qty);
    };
  });

  list.querySelectorAll("button[data-add-stock]").forEach((btn) => {
    btn.onclick = wrap(async () => {
      const id = btn.dataset.addStock;
      const input = list.querySelector(`input[data-stock-input="${id}"]`);
      const delta = Number(input.value || 0);
      if (!delta || delta < 0) return alert("Enter a positive stock increment");
      await addStock(id, delta);
    });
  });
}

function renderCart() {
  const el = document.getElementById("cart");
  if (!cart.length) {
    el.innerHTML = '<div class="muted">Cart is empty</div>';
  } else {
    el.innerHTML = cart.map((c) => `
      <div class="item-row">
        <div>${c.qty} x <code>${c.itemId}</code> @ $${c.price}</div>
        <button data-remove="${c.itemId}">Remove from cart</button>
      </div>
    `).join("");
    el.querySelectorAll("button[data-remove]").forEach((btn) => {
      btn.onclick = () => {
        const id = btn.dataset.remove;
        const idx = cart.findIndex((x) => x.itemId === id);
        if (idx >= 0) cart.splice(idx, 1);
        renderCart();
      };
    });
  }
  const total = cart.reduce((sum, c) => sum + c.qty * c.price, 0);
  document.getElementById("cartTotal").textContent = total;
}

function renderInventory() {
  const el = document.getElementById("inventoryList");
  if (!inventoryResults.length) {
    el.innerHTML = '<div class="muted">No inventory queried yet</div>';
    return;
  }
  el.innerHTML = inventoryResults.map((row) => {
    if (row.error) {
      return `<div class="item-row"><code>${row.id}</code> - error: ${row.error}</div>`;
    }
    return `<div class="item-row"><code>${row.id}</code> - stock: ${row.stock} - price: $${row.price}</div>`;
  }).join("");
}

function renderOpsDashboard(snapshot) {
  opsSnapshot = snapshot;
  document.getElementById("opsGeneratedAt").textContent =
    `Snapshot generated at ${snapshot.generated_at} from ${snapshot.source}`;

  document.getElementById("opsSummary").innerHTML = [
    {
      label: "Healthy instances",
      value: `${snapshot.summary.healthy_instances}/${snapshot.summary.total_instances}`,
      detail: `${snapshot.summary.degraded_instances} degraded instance(s) across ${snapshot.cluster.num_shards} shards`,
    },
    {
      label: "Saga backlog",
      value: formatNumber(snapshot.summary.active_sagas),
      detail: `${snapshot.summary.saga_failures} failed or compensating saga(s) need operator attention`,
    },
    {
      label: "2PL / 2PC in flight",
      value: formatNumber(snapshot.summary.active_2pc_transactions),
      detail: `${snapshot.summary.prepared_locks} prepared locks currently held across participants`,
    },
    {
      label: "Database reachability",
      value: `${snapshot.summary.reachable_databases}/${snapshot.summary.total_databases}`,
      detail: `${snapshot.summary.slowest_db_ms} ms slowest Redis ping across ${formatNumber(snapshot.summary.total_db_keys)} tracked keys`,
    },
  ].map((card) => `
    <div class="stat-card">
      <div class="stat-label">${card.label}</div>
      <div class="stat-value">${card.value}</div>
      <div class="stat-detail">${card.detail}</div>
    </div>
  `).join("");

  document.getElementById("serviceGrid").innerHTML = snapshot.service_instances.map((svc) => `
    <article class="service-card">
      <div class="service-top">
        <div>
          <div class="service-name">${svc.service} / shard ${svc.shard}</div>
          <div class="service-meta">${svc.pod} - ${svc.namespace} - mode ${svc.mode || snapshot.cluster.mode}</div>
        </div>
        <span class="pill ${statusClass(svc.status)}">${svc.status}</span>
      </div>
      <div class="service-stats">
        <div>DB ping<strong>${svc.db_ping_ms ?? "n/a"} ms</strong></div>
        <div>Redis keys<strong>${svc.redis_keys != null ? formatNumber(svc.redis_keys) : "n/a"}</strong></div>
        <div>Uptime<strong>${formatDuration(svc.uptime_seconds)}</strong></div>
        <div>DB status<strong>${svc.db_ok ? "reachable" : "error"}</strong></div>
      </div>
      ${svc.error ? `<div class="info muted">${svc.error}</div>` : ""}
    </article>
  `).join("");

  document.getElementById("dbGrid").innerHTML = snapshot.databases.map((db) => `
    <article class="db-card">
      <div class="db-top">
        <div>
          <div class="db-name">${db.name}</div>
          <div class="db-meta">${db.role} - shard ${db.shard} - ${db.pod || `${db.service}-shard-${db.shard}`}</div>
        </div>
        <span class="pill ${statusClass(db.status)}">${db.status}</span>
      </div>
      <div class="db-stats">
        <div>Reachable<strong>${db.reachable ? "yes" : "no"}</strong></div>
        <div>Ping<strong>${db.ping_ms ?? "n/a"} ms</strong></div>
        <div>Keys<strong>${db.key_count != null ? formatNumber(db.key_count) : "n/a"}</strong></div>
        <div>Uptime<strong>${formatDuration(db.uptime_seconds)}</strong></div>
      </div>
      ${db.error ? `<div class="info muted">${db.error}</div>` : ""}
    </article>
  `).join("");

  document.getElementById("sagaSummary").innerHTML = snapshot.sagas.status_breakdown.map((row) => `
    <article class="flow-card">
      <div class="flow-name">${row.status}</div>
      <div class="flow-count">${row.count}</div>
      <div class="flow-detail">${row.copy}</div>
    </article>
  `).join("");

  document.getElementById("sagaList").innerHTML = snapshot.sagas.recent.map((saga) => `
    <div class="item-row event-row">
      <div class="event-id"><code>${saga.order_id}</code></div>
      <div class="event-main">
        <div class="event-title">${saga.status} on shard ${saga.shard}</div>
        <div class="event-copy">${saga.copy}</div>
      </div>
      <span class="pill ${statusClass(saga.status)}">${saga.age_seconds}s</span>
    </div>
  `).join("");

  document.getElementById("twoplSummary").innerHTML = snapshot.twoplpc.status_breakdown.map((row) => `
    <article class="flow-card">
      <div class="flow-name">${row.status}</div>
      <div class="flow-count">${row.count}</div>
      <div class="flow-detail">${row.copy}</div>
    </article>
  `).join("");

  document.getElementById("twoplList").innerHTML = snapshot.twoplpc.recent.map((tx) => `
    <div class="item-row event-row">
      <div class="event-id"><code>${tx.order_id}</code></div>
      <div class="event-main">
        <div class="event-title">${tx.status} with ${tx.lock_count} lock(s)</div>
        <div class="event-copy">${tx.copy}</div>
        ${tx.failure_reason ? `<div class="event-copy"><strong>Failure reason:</strong> ${tx.failure_reason}</div>` : ""}
      </div>
      <span class="pill ${statusClass(tx.status)}">${tx.wait_ms} ms</span>
    </div>
  `).join("");
}

function getFallbackOpsSnapshot() {
  const service_instances = [
    { service: "order", shard: 0, namespace: "dds25", pod: "order-shard-0", mode: "saga", status: "healthy", db_ok: true, db_ping_ms: 4.2, redis_keys: 1450, uptime_seconds: 4812, error: null },
    { service: "order", shard: 1, namespace: "dds25", pod: "order-shard-1", mode: "saga", status: "degraded", db_ok: false, db_ping_ms: null, redis_keys: null, uptime_seconds: 397, error: "Redis connection timed out" },
    { service: "order", shard: 2, namespace: "dds25", pod: "order-shard-2", mode: "saga", status: "healthy", db_ok: true, db_ping_ms: 5.1, redis_keys: 1495, uptime_seconds: 5220, error: null },
    { service: "payment", shard: 0, namespace: "dds25", pod: "payment-shard-0", mode: "saga", status: "healthy", db_ok: true, db_ping_ms: 3.8, redis_keys: 1180, uptime_seconds: 7301, error: null },
    { service: "payment", shard: 1, namespace: "dds25", pod: "payment-shard-1", mode: "saga", status: "healthy", db_ok: true, db_ping_ms: 4.1, redis_keys: 1225, uptime_seconds: 6950, error: null },
    { service: "payment", shard: 2, namespace: "dds25", pod: "payment-shard-2", mode: "saga", status: "healthy", db_ok: true, db_ping_ms: 3.9, redis_keys: 1194, uptime_seconds: 6840, error: null },
    { service: "stock", shard: 0, namespace: "dds25", pod: "stock-shard-0", mode: "saga", status: "healthy", db_ok: true, db_ping_ms: 5.8, redis_keys: 2020, uptime_seconds: 6080, error: null },
    { service: "stock", shard: 1, namespace: "dds25", pod: "stock-shard-1", mode: "saga", status: "healthy", db_ok: true, db_ping_ms: 8.4, redis_keys: 2290, uptime_seconds: 5930, error: null },
    { service: "stock", shard: 2, namespace: "dds25", pod: "stock-shard-2", mode: "saga", status: "healthy", db_ok: true, db_ping_ms: 6.2, redis_keys: 2105, uptime_seconds: 6012, error: null },
  ];
  const databases = [
    { name: "order-db-0", role: "primary", shard: 0, pod: "order-shard-0", status: "healthy", reachable: true, ping_ms: 4.2, key_count: 1450, uptime_seconds: 4812, error: null },
    { name: "order-db-1", role: "primary", shard: 1, pod: "order-shard-1", status: "degraded", reachable: false, ping_ms: null, key_count: null, uptime_seconds: 397, error: "Redis connection timed out" },
    { name: "order-db-2", role: "primary", shard: 2, pod: "order-shard-2", status: "healthy", reachable: true, ping_ms: 5.1, key_count: 1495, uptime_seconds: 5220, error: null },
    { name: "payment-db-0", role: "primary", shard: 0, pod: "payment-shard-0", status: "healthy", reachable: true, ping_ms: 3.8, key_count: 1180, uptime_seconds: 7301, error: null },
    { name: "payment-db-1", role: "primary", shard: 1, pod: "payment-shard-1", status: "healthy", reachable: true, ping_ms: 4.1, key_count: 1225, uptime_seconds: 6950, error: null },
    { name: "payment-db-2", role: "primary", shard: 2, pod: "payment-shard-2", status: "healthy", reachable: true, ping_ms: 3.9, key_count: 1194, uptime_seconds: 6840, error: null },
    { name: "stock-db-0", role: "primary", shard: 0, pod: "stock-shard-0", status: "healthy", reachable: true, ping_ms: 5.8, key_count: 2020, uptime_seconds: 6080, error: null },
    { name: "stock-db-1", role: "primary", shard: 1, pod: "stock-shard-1", status: "healthy", reachable: true, ping_ms: 8.4, key_count: 2290, uptime_seconds: 5930, error: null },
    { name: "stock-db-2", role: "primary", shard: 2, pod: "stock-shard-2", status: "healthy", reachable: true, ping_ms: 6.2, key_count: 2105, uptime_seconds: 6012, error: null },
  ];
  return {
    generated_at: new Date().toISOString(),
    source: "ui-fallback",
    cluster: { namespace: "dds25", num_shards: 3, mode: "saga" },
    summary: {
      total_instances: 9,
      healthy_instances: 7,
      degraded_instances: 2,
      active_sagas: 18,
      saga_failures: 3,
      active_2pc_transactions: 6,
      prepared_locks: 11,
      reachable_databases: 8,
      total_databases: 9,
      slowest_db_ms: 8.4,
      total_db_keys: 12959,
    },
    service_instances,
    databases,
    sagas: {
      status_breakdown: [
        { status: "TRYING", count: 8, copy: "Fresh checkouts waiting on reservation responses." },
        { status: "RESERVED", count: 5, copy: "Both participants reserved; commit should follow quickly." },
        { status: "COMMITTED", count: 14, copy: "Recently completed sagas, useful for throughput context." },
        { status: "FAILED", count: 3, copy: "Compensated or stalled flows that deserve operator review." },
      ],
      recent: [
        { order_id: "saga-demo-1", status: "TRYING", shard: 1, age_seconds: 19, copy: "Payment reserve completed; stock reserve still pending." },
        { order_id: "saga-demo-2", status: "RESERVED", shard: 0, age_seconds: 31, copy: "Reservations held on both services, commit event not yet observed." },
        { order_id: "saga-demo-3", status: "FAILED", shard: 2, age_seconds: 54, copy: "Stock reservation timed out and compensation path was triggered." },
        { order_id: "saga-demo-4", status: "COMMITTED", shard: 2, age_seconds: 12, copy: "Order completed and paid flag should now be visible in the order shard." },
      ],
    },
    twoplpc: {
      status_breakdown: [
        { status: "PREPARING", count: 3, copy: "Coordinator is still collecting prepare votes from participants." },
        { status: "PREPARED", count: 4, copy: "Prepared locks are held; long dwell time should raise an alert." },
        { status: "COMMITTING", count: 2, copy: "Commit messages are in flight and waiting for acknowledgement." },
        { status: "ABORTED", count: 1, copy: "A participant exceeded deadline and the transaction was rolled back." },
      ],
      recent: [
        { order_id: "2pc-demo-1", status: "PREPARING", lock_count: 2, wait_ms: 140, copy: "Waiting on stock shard prepare acknowledgement." },
        { order_id: "2pc-demo-2", status: "PREPARED", lock_count: 3, wait_ms: 310, copy: "Locks held across payment and two stock shards." },
        { order_id: "2pc-demo-3", status: "COMMITTING", lock_count: 2, wait_ms: 220, copy: "Commit propagated to payment; stock commit still pending." },
        { order_id: "2pc-demo-4", status: "ABORTED", lock_count: 2, wait_ms: 480, copy: "Coordinator aborted after deadline exceeded during prepare phase." },
      ],
    },
  };
}

async function refreshOpsDashboard() {
  try {
    const snapshot = await get("/monitoring/overview");
    renderOpsDashboard(snapshot);
  } catch (err) {
    console.warn("Failed to load monitoring overview, using fallback", err);
    renderOpsDashboard(getFallbackOpsSnapshot());
    setInfo("opsGeneratedAt", "Showing UI fallback snapshot because the monitoring endpoint is unavailable.", true);
  }
}

function renderApiBaseControls() {
  document.getElementById("apiBaseInput").value = getApiBase();
  setInfo("apiBaseInfo", `Current base URL: ${getApiBase()}`, false);
}

function syncApiBaseQueryParam() {
  const url = new URL(window.location.href);
  if (getApiBase() === DEFAULT_API_BASE) url.searchParams.delete(API_BASE_QUERY_PARAM);
  else url.searchParams.set(API_BASE_QUERY_PARAM, getApiBase());
  window.history.replaceState({}, "", url);
}

function resetClientState() {
  userId = null;
  orderId = null;
  selectedUserId = null;
  users.length = 0;
  cart.length = 0;
  itemCache.clear();
  inventoryResults = [];
  opsSnapshot = null;

  renderUserList();
  renderItems();
  renderCart();
  renderInventory();
  setInfo("userInfo", "No user yet", true);
  setInfo("balanceInfo", "Pick a user above, then refresh.", true);
  setInfo("orderInfo", "No order yet", true);
}

function applyApiBase() {
  const input = document.getElementById("apiBaseInput");
  const previous = getApiBase();
  const next = normalizeApiBase(input.value);
  if (!next) {
    apiBase = DEFAULT_API_BASE;
    window.localStorage.removeItem(API_BASE_STORAGE_KEY);
  } else {
    apiBase = next;
    window.localStorage.setItem(API_BASE_STORAGE_KEY, apiBase);
  }
  syncApiBaseQueryParam();
  if (previous !== getApiBase()) resetClientState();
  renderApiBaseControls();
  return previous !== getApiBase();
}

function showView(name) {
  activeView = name === "ops" ? "ops" : "demo";
  window.localStorage.setItem(ACTIVE_VIEW_STORAGE_KEY, activeView);
  const demoVisible = name === "demo";
  document.getElementById("demoView").classList.toggle("hidden", !demoVisible);
  document.getElementById("opsView").classList.toggle("hidden", demoVisible);
  document.getElementById("showDemo").classList.toggle("active", demoVisible);
  document.getElementById("showOps").classList.toggle("active", !demoVisible);
  if (!demoVisible && !opsSnapshot) refreshOpsDashboard();
}

function addToCart(itemId, qty) {
  if (!itemCache.has(itemId)) return alert("Unknown item price");
  if (!qty) return alert("Quantity cannot be zero");
  const price = itemCache.get(itemId).price;
  const existing = cart.find((c) => c.itemId === itemId);
  if (existing) existing.qty += qty;
  else cart.push({ itemId, qty, price });
  for (let i = cart.length - 1; i >= 0; i -= 1) if (cart[i].qty === 0) cart.splice(i, 1);
  renderCart();
}

async function addStock(itemId, amount) {
  await post(`/stock/add/${itemId}/${amount}`);
  await refreshItemMeta(itemId);
  renderItems();
}

async function createUser() {
  const data = await post("/payment/create_user");
  await addUser(data.user_id);
}

async function addFunds() {
  if (!selectedUserId) return alert("Select or create a user first");
  const amt = Number(document.getElementById("fundAmount").value || 0);
  await post(`/payment/add_funds/${selectedUserId}/${amt}`);
  const u = await get(`/payment/find_user/${selectedUserId}`);
  setInfo("userInfo", `User: ${u.user_id} | credit ${u.credit}`, false);
  setInfo("balanceInfo", `Balance: ${u.credit}`, false);
}

async function createItem() {
  const price = Number(document.getElementById("itemPrice").value || 0);
  if (!price && price !== 0) return alert("Enter a price");
  const data = await post(`/stock/item/create/${price}`);
  itemCache.set(data.item_id, { price, stock: 0 });
  await refreshItemMeta(data.item_id);
  renderItems();
}

async function refreshItemMeta(itemId) {
  try {
    const res = await get(`/stock/find/${itemId}`);
    const meta = itemCache.get(itemId) || {};
    itemCache.set(itemId, { ...meta, price: res.price, stock: res.stock });
    renderInventory();
  } catch (err) {
    console.warn("Failed to refresh item", itemId, err);
  }
}

async function refreshBalance() {
  if (!selectedUserId) return alert("Select or create a user first");
  const u = await get(`/payment/find_user/${selectedUserId}`);
  setInfo("balanceInfo", `Balance: ${u.credit}`, false);
}

async function fetchInventory() {
  const input = document.getElementById("itemIds").value || "";
  const ids = input.split(/[\s,]+/).map((s) => s.trim()).filter(Boolean);
  if (!ids.length) return alert("Enter at least one item id");
  const results = [];
  for (const id of ids) {
    try {
      const res = await get(`/stock/find/${id}`);
      results.push({ id, stock: res.stock, price: res.price });
      itemCache.set(id, { price: res.price, stock: res.stock });
    } catch (err) {
      results.push({ id, error: err.message || "lookup failed" });
    }
  }
  inventoryResults = results;
  renderItems();
  renderInventory();
}

async function newOrder() {
  if (!selectedUserId) return alert("Select or create a user first");
  const data = await post(`/orders/create/${selectedUserId}`);
  orderId = data.order_id;
  cart.length = 0;
  renderCart();
  setInfo("orderInfo", `Order: ${orderId} | paid: false | total: $0 | items: 0`, false);
}

async function syncOrder() {
  if (!orderId) return alert("Create an order first");
  for (const c of cart) {
    await post(`/orders/addItem/${orderId}/${c.itemId}/${c.qty}`);
  }
  const o = await get(`/orders/find/${orderId}`);
  setInfo("orderInfo", formatOrder(o), false);
}

async function checkout() {
  if (!orderId) return alert("Create an order first");
  const resp = await post(`/orders/checkout/${orderId}`);
  alert(`Checkout response: ${JSON.stringify(resp)}`);
}

async function fetchOrder() {
  if (!orderId) return alert("Create an order first");
  const o = await get(`/orders/find/${orderId}`);
  setInfo("orderInfo", formatOrder(o), false);
}

function wireEvents() {
  document.getElementById("createUser").onclick = wrap(createUser);
  document.getElementById("addFunds").onclick = wrap(addFunds);
  document.getElementById("clearUsers").onclick = wrap(clearUsers);
  document.getElementById("createItem").onclick = wrap(createItem);
  document.getElementById("refreshBalance").onclick = wrap(refreshBalance);
  document.getElementById("fetchInventory").onclick = wrap(fetchInventory);
  document.getElementById("newOrder").onclick = wrap(newOrder);
  document.getElementById("syncOrder").onclick = wrap(syncOrder);
  document.getElementById("checkout").onclick = wrap(checkout);
  document.getElementById("fetchOrder").onclick = wrap(fetchOrder);
  document.getElementById("showDemo").onclick = () => showView("demo");
  document.getElementById("showOps").onclick = () => showView("ops");
  document.getElementById("refreshOps").onclick = wrap(refreshOpsDashboard);
  document.getElementById("applyApiBase").onclick = wrap(async () => {
    const changed = applyApiBase();
    if (changed && activeView === "ops") await refreshOpsDashboard();
  });
}

function wrap(fn) {
  return async () => {
    try {
      await fn();
    } catch (err) {
      alert(err.message || err);
      console.error(err);
    }
  };
}

renderItems();
renderCart();
renderUserList();
renderInventory();
renderApiBaseControls();
wireEvents();
showView(activeView);
