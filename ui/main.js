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
const cart = [];               // [{itemId, qty, price}]
const itemCache = new Map();   // itemId -> price

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

function formatOrder(o) {
  return `Order: ${o.order_id} | paid: ${o.paid} | total: $${o.total_cost} | items: ${o.items.length}`;
}

function renderItems() {
  const list = document.getElementById("itemList");
  if (itemCache.size === 0) {
    list.innerHTML = '<div class="muted">No items yet</div>';
    return;
  }
  list.innerHTML = [...itemCache.entries()].map(([id, price]) => `
    <div class="item-row">
      <div><code>${id}</code> — $${price}</div>
      <label>Qty <input type="number" data-id="${id}" value="1" step="1"></label>
      <button data-add="${id}">Add to cart</button>
      <button data-add="${id}" data-negative="1">Remove from order (-qty)</button>
    </div>
  `).join("");
  list.querySelectorAll("button[data-add]").forEach((btn) => {
    btn.onclick = () => {
      const id = btn.dataset.add;
      const input = list.querySelector(`input[data-id="${id}"]`);
      let qty = Number(input.value || 0);
      if (btn.dataset.negative) qty = -Math.abs(qty || 1);
      addToCart(id, qty);
    };
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

function addToCart(itemId, qty) {
  if (!itemCache.has(itemId)) return alert("Unknown item price");
  if (!qty) return alert("Quantity cannot be zero");
  const existing = cart.find((c) => c.itemId === itemId);
  if (existing) existing.qty += qty;
  else cart.push({ itemId, qty, price: itemCache.get(itemId) });
  for (let i = cart.length - 1; i >= 0; i -= 1) if (cart[i].qty === 0) cart.splice(i, 1);
  renderCart();
}

async function createUser() {
  const data = await post("/payment/create_user");
  userId = data.user_id;
  setInfo("userInfo", `User: ${userId} | credit 0`, false);
}

async function addFunds() {
  if (!userId) return alert("Create user first");
  const amt = Number(document.getElementById("fundAmount").value || 0);
  await post(`/payment/add_funds/${userId}/${amt}`);
  const u = await get(`/payment/find_user/${userId}`);
  setInfo("userInfo", `User: ${u.user_id} | credit ${u.credit}`, false);
}

async function createItem() {
  const price = Number(document.getElementById("itemPrice").value || 0);
  if (!price && price !== 0) return alert("Enter a price");
  const data = await post(`/stock/item/create/${price}`);
  itemCache.set(data.item_id, price);
  renderItems();
}

async function newOrder() {
  if (!userId) return alert("Create user first");
  const data = await post(`/orders/create/${userId}`);
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
  document.getElementById("createItem").onclick = wrap(createItem);
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
wireEvents();
