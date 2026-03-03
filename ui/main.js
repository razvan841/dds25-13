// UI logic for DDS demo
const API_BASE = (() => {
  const qs = new URLSearchParams(window.location.search);
  const override = qs.get("api");
  if (override) return override.replace(/\/$/, "");
  const host = window.location.hostname;
  if (host === "gateway") return "http://gateway:80";
  if (host === "localhost" || host === "127.0.0.1" || host === "") return "http://localhost:8000";
  return `${window.location.protocol}//${host}:8000`;
})();

let userId = null;
let orderId = null;
let selectedUserId = null;
const users = [];
const cart = [];               // [{itemId, qty, price}]
const itemCache = new Map();   // itemId -> {price, stock}
let inventoryResults = [];

async function request(method, path) {
  const res = await fetch(`${API_BASE}${path}`, { method });
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
  userId = id; // keep legacy variable in sync
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
        <div><code>${id}</code> — $${price} — stock: ${stock}</div>
        <label>Qty <input type="number" data-id="${id}" value="1" step="1"></label>
        <button data-add="${id}">Add to cart</button>
        <button data-add="${id}" data-negative="1">Remove from order (-qty)</button>
        <label class="inline">Stock Δ <input type="number" data-stock-input="${id}" value="5" step="1" min="1"></label>
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
        <div>${c.qty} × <code>${c.itemId}</code> @ $${c.price}</div>
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
      return `<div class="item-row"><code>${row.id}</code> — error: ${row.error}</div>`;
    }
    return `<div class="item-row"><code>${row.id}</code> — stock: ${row.stock} — price: $${row.price}</div>`;
  }).join("");
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
    renderInventory(); // update inventory card if same ids
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
  alert(`Checkout started. Response: ${JSON.stringify(resp)}`);
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
}

function wrap(fn) {
  return async () => {
    try { await fn(); }
    catch (err) { alert(err.message || err); console.error(err); }
  };
}

renderItems();
renderCart();
renderUserList();
renderInventory();
wireEvents();
