package com.capitec.kafka.portal;

public class PortalHtmlServer {

    public static String html() {
        return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Capitec Order Portal</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --bg:       #0f172a;
      --surface:  #1e293b;
      --border:   #334155;
      --muted:    #64748b;
      --text:     #e2e8f0;
      --text2:    #94a3b8;
      --primary:  #6366f1;
      --primary2: #4f46e5;
      --green:    #22c55e;
      --red:      #ef4444;
      --amber:    #f59e0b;
      --blue:     #38bdf8;
      --cyan:     #06b6d4;
      --orange:   #f97316;
    }
    body { font-family: system-ui,sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }

    /* ── NAV ── */
    nav { background: var(--surface); border-bottom: 1px solid var(--border); padding: 0 2rem; display:flex; align-items:center; height:56px; gap:2rem; }
    nav .brand { font-weight:800; font-size:1rem; color:var(--primary); letter-spacing:.05em; }
    nav .nav-links { display:flex; gap:0; }
    nav .nav-link { padding: 0 1rem; height:56px; display:flex; align-items:center; font-size:0.85rem; font-weight:600; color:var(--text2); cursor:pointer; border-bottom:2px solid transparent; transition:all .15s; }
    nav .nav-link.active, nav .nav-link:hover { color:var(--text); border-bottom-color:var(--primary); }
    nav .spacer { flex:1; }
    nav .user-chip { background:var(--bg); border:1px solid var(--border); border-radius:999px; padding:0.3rem 0.9rem; font-size:0.78rem; color:var(--text2); }
    nav .btn-sign-out { background:transparent; border:1px solid var(--border); color:var(--text2); border-radius:8px; padding:0.35rem 0.85rem; font-size:0.78rem; cursor:pointer; }
    nav .btn-sign-out:hover { border-color:var(--red); color:var(--red); }

    /* ── PAGES ── */
    .page { display:none; padding: 2rem; max-width:1100px; margin:0 auto; }
    .page.active { display:block; }

    /* ── AUTH CARDS ── */
    .auth-wrap { min-height:calc(100vh - 56px); display:flex; align-items:center; justify-content:center; padding:2rem; }
    .auth-card { background:var(--surface); border:1px solid var(--border); border-radius:14px; padding:2.5rem; width:100%; max-width:440px; }
    .auth-card h2 { font-size:1.3rem; font-weight:700; margin-bottom:0.25rem; }
    .auth-card p.sub { color:var(--text2); font-size:0.83rem; margin-bottom:1.75rem; }

    /* ── FORM ELEMENTS ── */
    .field { margin-top:1rem; }
    label { display:block; font-size:0.76rem; color:var(--text2); margin-bottom:0.35rem; }
    input, select { width:100%; background:var(--bg); border:1px solid var(--border); border-radius:8px; color:var(--text); padding:0.6rem 0.8rem; font-size:0.88rem; outline:none; transition:border-color .15s; }
    input:focus, select:focus { border-color:var(--primary); }
    .field-row { display:flex; gap:0.75rem; }
    .field-row .field { flex:1; }

    /* ── BUTTONS ── */
    .btn-primary { width:100%; background:var(--primary); color:#fff; border:none; border-radius:8px; padding:0.7rem; font-size:0.9rem; font-weight:700; cursor:pointer; margin-top:1.25rem; transition:background .15s; }
    .btn-primary:hover { background:var(--primary2); }
    .btn-primary:disabled { background:var(--border); color:var(--muted); cursor:not-allowed; }
    .btn-ghost { background:var(--surface); border:1px solid var(--border); color:var(--text2); border-radius:8px; padding:0.55rem 1.1rem; font-size:0.83rem; font-weight:600; cursor:pointer; transition:all .15s; }
    .btn-ghost:hover { background:var(--bg); color:var(--text); }
    .link-btn { background:none; border:none; color:var(--primary); cursor:pointer; font-size:0.83rem; padding:0; text-decoration:underline; }
    .link-btn:hover { color:var(--primary2); }
    .form-footer { text-align:center; margin-top:1rem; font-size:0.82rem; color:var(--text2); }

    /* ── ALERTS ── */
    .alert { border-radius:8px; padding:0.65rem 0.9rem; font-size:0.82rem; margin-top:1rem; display:none; }
    .alert.error { background:#2d0707; border:1px solid var(--red); color:#fca5a5; display:block; }
    .alert.success { background:#052e16; border:1px solid var(--green); color:#86efac; display:block; }

    /* ── PROFILE ── */
    .profile-grid { display:grid; grid-template-columns:1fr 2fr; gap:1.5rem; align-items:start; }
    .profile-card { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:1.75rem; }
    .profile-card h3 { font-size:1rem; font-weight:700; margin-bottom:1.25rem; }
    .profile-avatar { width:64px; height:64px; border-radius:50%; background:var(--primary); display:flex; align-items:center; justify-content:center; font-size:1.5rem; font-weight:800; color:#fff; margin-bottom:1rem; }
    .profile-name { font-size:1.1rem; font-weight:700; }
    .profile-cnum { font-size:0.78rem; color:var(--text2); font-family:monospace; margin-top:0.2rem; }
    .info-row { display:flex; justify-content:space-between; padding:0.5rem 0; border-bottom:1px solid var(--border); font-size:0.83rem; }
    .info-row:last-child { border-bottom:none; }
    .info-row .lbl { color:var(--text2); }

    /* ── ORDERS TIMELINE ── */
    .orders-card { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:1.75rem; }
    .orders-card h3 { font-size:1rem; font-weight:700; margin-bottom:1.25rem; }
    .order-item { display:flex; gap:1rem; padding:0.85rem 0; border-bottom:1px solid var(--border); align-items:center; }
    .order-item:last-child { border-bottom:none; }
    .order-id { font-family:monospace; font-size:0.78rem; color:var(--text); font-weight:700; flex:0 0 180px; }
    .order-details { flex:1; }
    .order-product { font-size:0.85rem; font-weight:600; }
    .order-amount { font-size:0.78rem; color:var(--text2); }
    .order-date { font-size:0.72rem; color:var(--muted); margin-top:0.15rem; }
    .empty-orders { text-align:center; padding:2rem; color:var(--muted); font-size:0.85rem; }

    /* ── PRODUCTS ── */
    .products-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(280px,1fr)); gap:1.25rem; }
    .product-card { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:1.5rem; display:flex; flex-direction:column; gap:0.75rem; transition:border-color .15s; }
    .product-card:hover { border-color:var(--primary); }
    .product-icon { font-size:2rem; }
    .product-name { font-size:1rem; font-weight:700; }
    .product-desc { font-size:0.8rem; color:var(--text2); line-height:1.5; }
    .product-meta { display:flex; gap:1rem; }
    .product-meta span { font-size:0.72rem; background:var(--bg); border:1px solid var(--border); border-radius:6px; padding:0.2rem 0.5rem; color:var(--text2); }
    .btn-apply { background:var(--primary); color:#fff; border:none; border-radius:8px; padding:0.55rem 1rem; font-size:0.83rem; font-weight:700; cursor:pointer; margin-top:auto; transition:background .15s; }
    .btn-apply:hover { background:var(--primary2); }

    /* ── MODAL ── */
    .modal-overlay { position:fixed; inset:0; background:rgba(0,0,0,0.7); display:none; align-items:center; justify-content:center; z-index:100; }
    .modal-overlay.open { display:flex; }
    .modal { background:var(--surface); border:1px solid var(--border); border-radius:14px; padding:2rem; width:100%; max-width:420px; }
    .modal h3 { font-size:1rem; font-weight:700; margin-bottom:1.25rem; }
    .modal-footer { display:flex; gap:0.75rem; margin-top:1.25rem; }

    /* ── STATUS BADGES ── */
    .badge { display:inline-block; padding:0.15rem 0.55rem; border-radius:999px; font-size:0.68rem; font-weight:700; text-transform:uppercase; letter-spacing:.04em; }
    .badge-PENDING          { background:#451a03; color:var(--amber); }
    .badge-CONFIRMED        { background:#1e1b4b; color:#818cf8; }
    .badge-PAYMENT-INIT     { background:#052e16; color:var(--green); }
    .badge-PAYMENT-PROCESSED{ background:#0c1a2e; color:var(--blue); }
    .badge-PACKED           { background:#042f2e; color:var(--cyan); }
    .badge-OUT-FOR-DELIVERY { background:#431407; color:var(--orange); }
    .badge-DELIVERED        { background:#1e293b; color:var(--text2); }

    /* ── TOAST ── */
    .toast { position:fixed; bottom:1.5rem; right:1.5rem; background:var(--green); color:#fff; padding:0.6rem 1.1rem; border-radius:8px; font-size:0.83rem; font-weight:600; opacity:0; pointer-events:none; transition:opacity .3s; z-index:200; }
    .toast.show { opacity:1; }
    .toast.error { background:var(--red); }

    /* ── PAGE HEADER ── */
    .page-header { margin-bottom:2rem; }
    .page-header h2 { font-size:1.3rem; font-weight:700; }
    .page-header p  { color:var(--text2); font-size:0.85rem; margin-top:0.25rem; }

    /* ── CART BADGE ── */
    .cart-btn { position:relative; background:var(--surface); border:1px solid var(--border); color:var(--text2); border-radius:8px; padding:0.35rem 0.85rem; font-size:0.78rem; cursor:pointer; transition:all .15s; }
    .cart-btn:hover { border-color:var(--primary); color:var(--text); }
    .cart-badge { position:absolute; top:-6px; right:-6px; background:var(--primary); color:#fff; border-radius:999px; font-size:0.6rem; font-weight:800; min-width:16px; height:16px; display:flex; align-items:center; justify-content:center; padding:0 3px; }

    /* ── CART DRAWER ── */
    .drawer-overlay { position:fixed; inset:0; background:rgba(0,0,0,0.6); z-index:300; display:none; }
    .drawer-overlay.open { display:block; }
    .drawer { position:fixed; top:0; right:0; bottom:0; width:420px; max-width:100vw; background:var(--surface); border-left:1px solid var(--border); z-index:301; display:flex; flex-direction:column; transform:translateX(100%); transition:transform .25s ease; }
    .drawer.open { transform:translateX(0); }
    .drawer-head { padding:1.25rem 1.5rem; border-bottom:1px solid var(--border); display:flex; align-items:center; justify-content:space-between; }
    .drawer-head h3 { font-size:1rem; font-weight:700; }
    .drawer-close { background:none; border:none; color:var(--muted); font-size:1.2rem; cursor:pointer; line-height:1; }
    .drawer-close:hover { color:var(--red); }
    .drawer-body { flex:1; overflow-y:auto; padding:1rem 1.5rem; }
    .cart-item { display:flex; gap:0.75rem; padding:0.85rem 0; border-bottom:1px solid var(--border); align-items:flex-start; }
    .cart-item:last-child { border-bottom:none; }
    .cart-item-info { flex:1; }
    .cart-item-name { font-size:0.85rem; font-weight:600; color:var(--text); }
    .cart-item-sku  { font-size:0.68rem; color:var(--muted); font-family:monospace; margin:0.1rem 0; }
    .cart-item-price { font-size:0.82rem; color:var(--green); font-weight:700; }
    .cart-qty { display:flex; align-items:center; gap:0.4rem; }
    .cart-qty button { background:var(--bg); border:1px solid var(--border); color:var(--text); border-radius:6px; width:24px; height:24px; cursor:pointer; font-size:0.85rem; display:flex; align-items:center; justify-content:center; }
    .cart-qty button:hover { border-color:var(--primary); }
    .cart-qty span { font-size:0.85rem; font-weight:600; min-width:20px; text-align:center; }
    .cart-remove { background:none; border:none; color:var(--muted); cursor:pointer; font-size:0.75rem; margin-top:0.3rem; padding:0; }
    .cart-remove:hover { color:var(--red); }
    .drawer-foot { padding:1.25rem 1.5rem; border-top:1px solid var(--border); }
    .cart-total-row { display:flex; justify-content:space-between; margin-bottom:1rem; font-size:0.92rem; }
    .cart-total-row .label { color:var(--text2); }
    .cart-total-row .val { font-weight:800; font-size:1.1rem; color:var(--text); }
    .cart-empty-msg { text-align:center; padding:3rem 0; color:var(--muted); font-size:0.85rem; }
  </style>
</head>
<body>

<!-- NAV (hidden on auth pages) -->
<nav id="main-nav" style="display:none">
  <span class="brand">CAPITEC ORDER PORTAL</span>
  <div class="nav-links">
    <div class="nav-link active" onclick="showPage('home')">Home</div>
    <div class="nav-link" onclick="showPage('products')">Shop</div>
  </div>
  <span class="spacer"></span>
  <span class="user-chip" id="nav-name"></span>
  <button class="cart-btn" onclick="openCart()">
    🛒 Cart
    <span class="cart-badge" id="cart-badge" style="display:none">0</span>
  </button>
  <button class="btn-sign-out" onclick="signOut()">Sign out</button>
</nav>

<!-- ── CART DRAWER ── -->
<div class="drawer-overlay" id="drawer-overlay" onclick="closeCart()"></div>
<div class="drawer" id="cart-drawer" onkeydown="if(event.key==='Enter')checkout()">
  <div class="drawer-head">
    <h3>Your Cart</h3>
    <button class="drawer-close" onclick="closeCart()">✕</button>
  </div>
  <div class="drawer-body" id="cart-body"></div>
  <div class="drawer-foot" id="cart-foot" style="display:none">
    <div class="cart-total-row">
      <span class="label">Total</span>
      <span class="val" id="cart-total-display">R 0.00</span>
    </div>
    <button class="btn-primary" style="margin-top:0" onclick="checkout()">Checkout →</button>
  </div>
</div>

<!-- ── LOGIN PAGE ── -->
<div class="page active" id="page-login">
  <div class="auth-wrap">
    <div class="auth-card">
      <h2>Welcome back</h2>
      <p class="sub">Sign in with your cell number, email address, or customer number</p>
      <div id="login-alert" class="alert"></div>
      <div class="field"><label>Cell / Email / Customer Number</label><input id="login-cell" type="text" placeholder="e.g. 0821234567 or jane@email.com or 1000000000" autocomplete="off" onkeydown="if(event.key==='Enter')document.getElementById('login-password').focus()"/></div>
      <div class="field"><label>Password</label><input id="login-password" type="password" placeholder="••••••••" autocomplete="new-password" onkeydown="if(event.key==='Enter')doLogin()"/></div>
      <button class="btn-primary" onclick="doLogin()">Sign In</button>
      <p class="form-footer">No account? <button class="link-btn" onclick="goToSignup()">Register here</button></p>
    </div>
  </div>
</div>

<!-- ── SIGNUP PAGE ── -->
<div class="page" id="page-signup">
  <div class="auth-wrap">
    <div class="auth-card" style="max-width:520px">
      <h2>Create account</h2>
      <p class="sub">Fill in your details to get started</p>
      <div id="signup-alert" class="alert"></div>
      <div class="field-row">
        <div class="field"><label>First Name</label><input id="su-first" type="text" placeholder="Jane"/></div>
        <div class="field"><label>Last Name</label><input id="su-last" type="text" placeholder="Smith"/></div>
      </div>
      <div class="field"><label>ID Number</label><input id="su-id" type="text" placeholder="8001010000000"/></div>
      <div class="field"><label>Email</label><input id="su-email" type="email" placeholder="jane@example.com"/></div>
      <div class="field"><label>Cell Number</label><input id="su-cell" type="tel" placeholder="0821234567"/></div>
      <div class="field-row">
        <div class="field"><label>Password</label><input id="su-pwd" type="password" placeholder="••••••••"/></div>
        <div class="field"><label>Confirm Password</label><input id="su-pwd2" type="password" placeholder="••••••••"/></div>
      </div>
      <button class="btn-primary" onclick="doRegister()">Create Account</button>
      <p class="form-footer">Already registered? <button class="link-btn" onclick="goToLogin()">Sign in</button></p>
    </div>
  </div>
</div>

<!-- ── HOME PAGE ── -->
<div class="page" id="page-home">
  <div class="page-header"><h2 id="home-greeting">Welcome</h2><p>Manage your profile and view your orders</p></div>
  <div class="profile-grid">
    <div>
      <div class="profile-card">
        <div class="profile-avatar" id="profile-avatar">J</div>
        <div class="profile-name" id="profile-name">—</div>
        <div class="profile-cnum" id="profile-cnum">—</div>
        <div style="margin-top:1rem">
          <div class="info-row"><span class="lbl">ID Number</span><span id="p-idnumber">—</span></div>
          <div class="info-row"><span class="lbl">Email</span><span id="p-email">—</span></div>
          <div class="info-row"><span class="lbl">Cell</span><span id="p-cell">—</span></div>
        </div>
        <button class="btn-ghost" style="margin-top:1.25rem;width:100%" onclick="openEditModal()">Edit Profile</button>
      </div>
    </div>
    <div>
      <div class="orders-card">
        <h3>My Orders</h3>
        <div id="orders-list"><div class="empty-orders">Loading orders…</div></div>
      </div>
    </div>
  </div>
</div>

<!-- ── PRODUCTS PAGE ── -->
<div class="page" id="page-products">
  <div class="page-header">
    <div style="display:flex;align-items:center;gap:0.75rem">
      <button class="btn-ghost" id="back-btn" style="display:none;padding:0.35rem 0.75rem;font-size:0.8rem" onclick="showCategories()">← Back</button>
      <div><h2 id="products-heading">Shop by Category</h2><p id="products-sub">Select a category to browse parts</p></div>
    </div>
  </div>
  <div class="products-grid" id="products-grid"></div>
</div>

<!-- ── EDIT PROFILE MODAL ── -->
<div class="modal-overlay" id="edit-modal">
  <div class="modal">
    <h3>Edit Profile</h3>
    <div class="field"><label>First Name</label><input id="ep-first" type="text" onkeydown="if(event.key==='Enter')document.getElementById('ep-last').focus()"/></div>
    <div class="field"><label>Last Name</label><input id="ep-last" type="text" onkeydown="if(event.key==='Enter')document.getElementById('ep-email').focus()"/></div>
    <div class="field"><label>Email</label><input id="ep-email" type="email" onkeydown="if(event.key==='Enter')document.getElementById('ep-cell').focus()"/></div>
    <div class="field"><label>Cell</label><input id="ep-cell" type="tel" onkeydown="if(event.key==='Enter')saveProfile()"/></div>
    <div class="modal-footer">
      <button class="btn-ghost" onclick="closeModal('edit-modal')">Cancel</button>
      <button class="btn-primary" style="margin-top:0;flex:1" onclick="saveProfile()">Save</button>
    </div>
  </div>
</div>

<!-- ── CANCEL MODAL ── -->
<div class="modal-overlay" id="cancel-modal">
  <div class="modal">
    <h3>Cancel Order</h3>
    <p id="cancel-order-id" style="color:var(--muted);font-size:0.78rem;font-family:monospace;margin-bottom:1rem"></p>
    <div class="field">
      <label>Reason for cancellation</label>
      <select id="cancel-reason-select" onchange="updateCancelReason()">
        <option value="Customer changed mind">Changed my mind</option>
        <option value="Found cheaper elsewhere">Found cheaper elsewhere</option>
        <option value="Ordered by mistake">Ordered by mistake</option>
        <option value="Delivery takes too long">Delivery takes too long</option>
        <option value="Other">Other (specify below)</option>
      </select>
    </div>
    <div class="field" id="cancel-other-field" style="display:none">
      <label>Please specify</label>
      <input id="cancel-other" type="text" placeholder="Enter reason…" onkeydown="if(event.key==='Enter')submitCancel()"/>
    </div>
    <div id="cancel-alert" class="alert"></div>
    <div class="modal-footer">
      <button class="btn-ghost" onclick="closeModal('cancel-modal')">Keep Order</button>
      <button class="btn-primary" style="margin-top:0;flex:1;background:var(--red)" onclick="submitCancel()">Cancel Order</button>
    </div>
  </div>
</div>

<!-- ── ORDER MODAL ── -->
<div class="modal-overlay" id="apply-modal">
  <div class="modal">
    <div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:1rem">
      <div id="apply-icon" style="font-size:2rem"></div>
      <div>
        <h3 id="apply-title" style="margin:0"></h3>
        <p id="apply-sku" style="color:var(--muted);font-size:0.72rem;font-family:monospace"></p>
      </div>
      <div style="margin-left:auto;text-align:right">
        <div id="apply-price" style="font-size:1.4rem;font-weight:800;color:var(--green)"></div>
        <div style="font-size:0.7rem;color:var(--muted)">per unit</div>
      </div>
    </div>
    <p id="apply-desc" style="color:var(--text2);font-size:0.82rem;margin-bottom:1rem;line-height:1.5"></p>
    <div class="field">
      <label>Quantity</label>
      <div style="display:flex;gap:0.5rem;align-items:center">
        <select id="apply-qty-select" onchange="onQtySelect()" style="flex:0 0 auto;width:90px">
          <option value="1">1</option>
          <option value="2">2</option>
          <option value="3">3</option>
          <option value="4">4</option>
          <option value="5">5</option>
          <option value="6">6</option>
          <option value="8">8</option>
          <option value="10">10</option>
          <option value="custom">Other…</option>
        </select>
        <input id="apply-qty-custom" type="number" min="1" max="999" placeholder="Enter qty"
          style="display:none;flex:1" oninput="updateTotal()"
          onkeydown="if(event.key==='Enter')submitOrder()"/>
        <input id="apply-qty" type="hidden" value="1"/>
      </div>
    </div>
    <div id="apply-total" style="margin-top:0.75rem;padding:0.65rem 0.85rem;background:var(--bg);border:1px solid var(--border);border-radius:8px;display:flex;justify-content:space-between;font-size:0.85rem">
      <span style="color:var(--text2)">Order total</span>
      <span id="apply-total-val" style="font-weight:700;color:var(--text)">R 0.00</span>
    </div>
    <div id="apply-alert" class="alert"></div>
    <div class="modal-footer">
      <button class="btn-ghost" onclick="closeModal('apply-modal')">Cancel</button>
      <button class="btn-primary" style="margin-top:0;flex:1" onclick="submitOrder()">Add to Cart</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
  const CATALOGUE = {
    TYRES: {
      icon: '🛞', name: 'Tyres', desc: 'All-season, performance and off-road tyres for every vehicle.',
      variants: [
        { id:'TYRE_175_65_R14',  sku:'TYR-175-65-R14',  name:'175/65 R14 — Economy',       desc:'Everyday commuter tyre. Quiet ride, good wet grip. Fits most hatchbacks.',           price: 849  },
        { id:'TYRE_195_65_R15',  sku:'TYR-195-65-R15',  name:'195/65 R15 — Standard',       desc:'Popular mid-range tyre for sedans and small SUVs. Balanced performance.',            price: 1149 },
        { id:'TYRE_205_55_R16',  sku:'TYR-205-55-R16',  name:'205/55 R16 — Performance',    desc:'Sport compound for enhanced cornering. Low rolling resistance.',                      price: 1549 },
        { id:'TYRE_235_65_R17',  sku:'TYR-235-65-R17',  name:'235/65 R17 — SUV All-Season', desc:'Reinforced sidewall for SUVs and crossovers. Handles gravel and tar.',               price: 2199 },
        { id:'TYRE_265_70_R17',  sku:'TYR-265-70-R17',  name:'265/70 R17 — 4x4 Off-Road',  desc:'Aggressive tread pattern for off-road and light bush. Mud terrain rated.',            price: 3299 },
        { id:'TYRE_RUN_FLAT_17', sku:'TYR-RF-205-17',   name:'205/45 R17 — Run-Flat',       desc:'Continue driving up to 80 km at 80 km/h after a puncture. Fits BMW 3-Series.',       price: 2899 },
      ]
    },
    BRAKES: {
      icon: '🔴', name: 'Brakes', desc: 'Brake pads, discs and kits for all makes.',
      variants: [
        { id:'BRAKE_PAD_FRONT_STD',   sku:'BRK-PAD-FS',   name:'Front Brake Pads — Standard',     desc:'OEM-grade organic compound. Suitable for daily driving. Set of 4.',             price:  649 },
        { id:'BRAKE_PAD_FRONT_PERF',  sku:'BRK-PAD-FP',   name:'Front Brake Pads — Performance',  desc:'Semi-metallic compound for improved bite and fade resistance. Track-suitable.',  price: 1199 },
        { id:'BRAKE_PAD_REAR_STD',    sku:'BRK-PAD-RS',   name:'Rear Brake Pads — Standard',      desc:'OEM replacement rear pads. Includes shim anti-squeal kit. Set of 4.',           price:  549 },
        { id:'BRAKE_DISC_FRONT',      sku:'BRK-DSC-F',    name:'Front Brake Disc — Vented',       desc:'Solid vented disc for most sedans and hatchbacks. 280 mm diameter.',             price: 1350 },
        { id:'BRAKE_DISC_REAR',       sku:'BRK-DSC-R',    name:'Rear Brake Disc — Solid',         desc:'Solid rear disc. 260 mm. Fits most mid-range sedans.',                          price:  980 },
        { id:'BRAKE_KIT_FULL',        sku:'BRK-KIT-FULL', name:'Complete Brake Kit — Front+Rear', desc:'Front and rear pads + discs in one kit. Best value for full replacement.',       price: 4499 },
      ]
    },
    BATTERIES: {
      icon: '🔋', name: 'Batteries', desc: 'Reliable starter and AGM batteries for passenger and commercial vehicles.',
      variants: [
        { id:'BATT_NS40',    sku:'BAT-NS40',    name:'NS40 — 35Ah Compact',    desc:'Hatchbacks and small vehicles. Maintenance-free. 18-month warranty.',       price:  899 },
        { id:'BATT_N60',     sku:'BAT-N60',     name:'N60 — 60Ah Standard',    desc:'Most popular fitment. Suits sedans, bakkies and light vans.',                price: 1299 },
        { id:'BATT_N70',     sku:'BAT-N70',     name:'N70 — 70Ah Heavy Duty',  desc:'High cold-cranking amps for diesel engines and 4x4s.',                       price: 1699 },
        { id:'BATT_AGM_70',  sku:'BAT-AGM-70',  name:'AGM 70Ah — Start-Stop',  desc:'Required for vehicles with start-stop technology. Deep cycle capable.',      price: 2799 },
        { id:'BATT_AGM_95',  sku:'BAT-AGM-95',  name:'AGM 95Ah — Premium',     desc:'Top-spec AGM for luxury vehicles with high electrical demand.',              price: 3499 },
      ]
    },
    FILTERS: {
      icon: '🔧', name: 'Filters', desc: 'Oil, air, fuel and cabin filters from leading brands.',
      variants: [
        { id:'FILTER_OIL_STD',    sku:'FLT-OIL-S',   name:'Oil Filter — Standard',        desc:'Spin-on oil filter for most petrol engines. Includes seal.',                  price:  129 },
        { id:'FILTER_OIL_PREM',   sku:'FLT-OIL-P',   name:'Oil Filter — Premium Synthetic',desc:'Extended-life synthetic media. For use with full-synthetic engine oils.',    price:  249 },
        { id:'FILTER_AIR_PANEL',  sku:'FLT-AIR-PNL', name:'Panel Air Filter',              desc:'Direct replacement for standard air box. Improves airflow slightly.',         price:  299 },
        { id:'FILTER_AIR_PERF',   sku:'FLT-AIR-PRF', name:'High-Flow Performance Air Filter',desc:'Washable and reusable. Increases air intake for improved power.',          price:  799 },
        { id:'FILTER_FUEL',       sku:'FLT-FUEL',    name:'Inline Fuel Filter',            desc:'Prevents sediment reaching injectors. Replace every 30 000 km.',             price:  199 },
        { id:'FILTER_CABIN',      sku:'FLT-CAB',     name:'Cabin Pollen/Dust Filter',      desc:'Blocks dust, pollen and odours from the cabin. Easy DIY replacement.',        price:  259 },
      ]
    },
    WIPERS: {
      icon: '🌧️', name: 'Wipers', desc: 'Flat blade, conventional and rear wipers for all vehicles.',
      variants: [
        { id:'WIPER_FLAT_600',  sku:'WPR-FL-600',  name:'Flat Blade 600mm — Driver',    desc:'Aerodynamic flat blade. No-chatter coating. Universal fitting.',           price:  229 },
        { id:'WIPER_FLAT_400',  sku:'WPR-FL-400',  name:'Flat Blade 400mm — Passenger', desc:'Passenger side flat blade. Spring steel spine for even pressure.',         price:  199 },
        { id:'WIPER_CONV_PAIR', sku:'WPR-CV-PR',   name:'Conventional Pair (Front)',     desc:'Budget-friendly traditional wiper pair. Fits most SA vehicles.',           price:  299 },
        { id:'WIPER_REAR',      sku:'WPR-REAR',    name:'Rear Wiper Blade — 300mm',     desc:'Short rear blade for hatchbacks and SUVs. Includes adapter.',              price:  149 },
        { id:'WIPER_ALL_SEASON',sku:'WPR-AS-PAIR', name:'All-Season Hybrid Pair',        desc:'Silicone rubber compound. Works in heat, cold and heavy rain.',            price:  549 },
      ]
    },
    SHOCKS: {
      icon: '🏎️', name: 'Shock Absorbers', desc: 'Gas, twin-tube and sport shocks for a smooth controlled ride.',
      variants: [
        { id:'SHOCK_FRONT_GAS',    sku:'SHK-FR-GAS',  name:'Front Gas Shock — Standard',    desc:'Monotube gas shock absorber. Direct OEM replacement. Single unit.',         price:  899 },
        { id:'SHOCK_REAR_GAS',     sku:'SHK-RR-GAS',  name:'Rear Gas Shock — Standard',     desc:'Rear gas unit for improved handling and comfort. Single unit.',             price:  799 },
        { id:'SHOCK_FRONT_SPORT',  sku:'SHK-FR-SPT',  name:'Front Sport Shock — Lowered',   desc:'Firm sport-tuned damper for lowered vehicles. 30 mm drop. Single unit.',    price: 1499 },
        { id:'SHOCK_REAR_SPORT',   sku:'SHK-RR-SPT',  name:'Rear Sport Shock — Lowered',    desc:'Matched rear sport damper. Pairs with front sport shock.',                  price: 1299 },
        { id:'SHOCK_KIT_FULL',     sku:'SHK-KIT-4',   name:'Full Set — 4 Gas Shocks',       desc:'Complete set of 4 standard gas shocks. Best value, single fitment job.',    price: 2999 },
        { id:'SHOCK_BAKKIE_HEAVY', sku:'SHK-BK-HD',   name:'Heavy Duty Bakkie Shock',       desc:'Rated for laden bakkies and off-road use. Reinforced body. Per unit.',      price: 1799 },
      ]
    },
    LIGHTING: {
      icon: '💡', name: 'Lighting', desc: 'Headlight bulbs, LEDs and auxiliary lights.',
      variants: [
        { id:'BULB_H4_HALOGEN',  sku:'LGT-H4-HAL',  name:'H4 Halogen Bulb — Twin Pack',  desc:'Standard replacement 60/55W H4. Fits most SA vehicles. Twin pack.',        price:  179 },
        { id:'BULB_H7_HALOGEN',  sku:'LGT-H7-HAL',  name:'H7 Halogen Bulb — Twin Pack',  desc:'55W H7 low-beam replacement for modern European vehicles.',               price:  229 },
        { id:'BULB_H4_XENON',    sku:'LGT-H4-XEN',  name:'H4 Xenon-White Bulb — Twin',   desc:'5000K white output, factory-look upgrade. +30% more road visibility.',     price:  399 },
        { id:'BULB_LED_H7',      sku:'LGT-LED-H7',  name:'LED H7 Conversion — Twin',     desc:'High-output LED conversion kit. 6000K daylight white. IP67 waterproof.',   price: 1299 },
        { id:'LIGHT_LED_BAR',    sku:'LGT-LED-BAR', name:'LED Light Bar — 120W 20"',      desc:'Off-road spot/flood combo LED bar. Includes wiring harness and brackets.',  price: 1999 },
        { id:'LIGHT_REVERSE_LED',sku:'LGT-REV-LED', name:'LED Reverse Light Pair',        desc:'Plug-and-play LED reverse lights. Brighter and longer-lasting than halogens.',price: 499 },
      ]
    },
    OILS: {
      icon: '🛢️', name: 'Engine Oils', desc: 'Synthetic, semi-synthetic and mineral engine oils.',
      variants: [
        { id:'OIL_5W30_1L',   sku:'OIL-5W30-1',   name:'5W-30 Full Synthetic — 1L',      desc:'API SN/CF. For modern petrol and diesel engines. Excellent cold-start.',    price:  219 },
        { id:'OIL_5W30_5L',   sku:'OIL-5W30-5',   name:'5W-30 Full Synthetic — 5L',      desc:'Full synthetic 5-litre. Best value for complete oil changes.',              price:  899 },
        { id:'OIL_10W40_5L',  sku:'OIL-10W40-5',  name:'10W-40 Semi-Synthetic — 5L',     desc:'Good protection for older engines and high-mileage vehicles.',             price:  649 },
        { id:'OIL_15W40_5L',  sku:'OIL-15W40-5',  name:'15W-40 Mineral — 5L',            desc:'Mineral oil for carburettor engines and pre-2005 vehicles.',               price:  399 },
        { id:'OIL_0W20_5L',   sku:'OIL-0W20-5',   name:'0W-20 Full Synthetic — 5L',      desc:'Ultra-thin oil for start-stop and hybrid vehicles. Reduces fuel consumption.',price: 1099 },
        { id:'OIL_DIESEL_5L', sku:'OIL-DSL-5W40', name:'5W-40 Diesel Synthetic — 5L',    desc:'Specifically formulated for common-rail diesel engines.',                   price:  979 },
      ]
    },
  };

  let session = null;
  let currentProduct = null;
  let currentCategory = null;
  let cart = []; // [{variant, qty, category}]

  // ── Auth ──────────────────────────────────────────────────────────────────
  async function doLogin() {
    const cell     = document.getElementById('login-cell').value.trim();
    const password = document.getElementById('login-password').value;
    if (!cell || !password) { setAlert('login-alert', 'error', 'Cell and password required'); return; }
    clearAlert('login-alert');

    const res  = await post('/api/login', { cell, password });
    const data = await res.json();
    if (!res.ok) { setAlert('login-alert', 'error', data.error || 'Login failed'); return; }

    session = data;
    afterLogin();
  }

  async function doRegister() {
    const firstName = document.getElementById('su-first').value.trim();
    const lastName  = document.getElementById('su-last').value.trim();
    const idNumber  = document.getElementById('su-id').value.trim();
    const email     = document.getElementById('su-email').value.trim();
    const cell      = document.getElementById('su-cell').value.trim();
    const pwd       = document.getElementById('su-pwd').value;
    const pwd2      = document.getElementById('su-pwd2').value;

    if (!firstName || !lastName || !cell || !pwd) { setAlert('signup-alert', 'error', 'All fields required'); return; }
    if (pwd !== pwd2) { setAlert('signup-alert', 'error', 'Passwords do not match'); return; }
    clearAlert('signup-alert');

    const res  = await post('/api/register', { firstName, lastName, idNumber, email, cell, password: pwd });
    const data = await res.json();
    if (!res.ok) { setAlert('signup-alert', 'error', data.error || 'Registration failed'); return; }

    toast('Account created! Customer #' + data.customerNumber + '. Please sign in.');
    goToLogin();
  }

  function afterLogin() {
    document.getElementById('main-nav').style.display = '';
    document.getElementById('nav-name').textContent = session.firstName + '  #' + session.customerNumber;
    showPage('home');
    loadProfile();
    loadOrders();
  }

  async function signOut() {
    await post('/api/logout', {}, true);
    session = null;
    cart = [];
    updateCartBadge();
    closeCart();
    document.getElementById('main-nav').style.display = 'none';
    document.getElementById('login-cell').value = '';
    document.getElementById('login-password').value = '';
    showPage('login');
  }

  // ── Navigation ────────────────────────────────────────────────────────────
  function showPage(name) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.getElementById('page-' + name).classList.add('active');
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    const map = { home:0, products:1 };
    if (map[name] !== undefined) document.querySelectorAll('.nav-link')[map[name]].classList.add('active');
    if (name === 'home') { loadProfile(); loadOrders(); }
    if (name === 'products') { loadStock().then(() => showCategories()); }
  }

  function goToSignup() { document.querySelectorAll('.page').forEach(p=>p.classList.remove('active')); document.getElementById('page-signup').classList.add('active'); }
  function goToLogin()  { document.querySelectorAll('.page').forEach(p=>p.classList.remove('active')); document.getElementById('page-login').classList.add('active'); }

  // ── Profile ───────────────────────────────────────────────────────────────
  async function loadProfile() {
    const res = await fetch('/api/me', { credentials:'include' });
    if (!res.ok) return;
    const c = await res.json();
    const fn = c.firstName || c.first_name || '';
    const ln = c.lastName  || c.last_name  || '';
    document.getElementById('home-greeting').textContent   = 'Welcome, ' + fn;
    document.getElementById('profile-avatar').textContent  = (fn[0]||'?').toUpperCase();
    document.getElementById('profile-name').textContent    = fn + ' ' + ln;
    document.getElementById('profile-cnum').textContent    = 'Customer #' + (c.customerNumber || c.customer_number || '');
    document.getElementById('p-idnumber').textContent      = c.idNumber  || c.id_number || '—';
    document.getElementById('p-email').textContent         = c.email     || '—';
    document.getElementById('p-cell').textContent          = c.cell      || '—';
    // pre-fill edit modal
    document.getElementById('ep-first').value = fn;
    document.getElementById('ep-last').value  = ln;
    document.getElementById('ep-email').value = c.email || '';
    document.getElementById('ep-cell').value  = c.cell  || '';
  }

  async function loadOrders() {
    const res  = await fetch('/api/my-orders', { credentials:'include' });
    const data = await res.json();
    const list = data.orders || [];
    const el   = document.getElementById('orders-list');
    if (!list.length) { el.innerHTML = '<div class="empty-orders">No orders yet.</div>'; return; }
    const cancellable = ['CONFIRMED','PAYMENT-INIT','PAYMENT-PROCESSED','PACKED'];
    el.innerHTML = list.map(o => `
      <div class="order-item">
        <div class="order-id">${o.orderID}</div>
        <div class="order-details">
          <div class="order-product">${fmt(o.product)}</div>
          <div class="order-amount">R ${Number(o.amount).toLocaleString('en-ZA',{minimumFractionDigits:2})}</div>
          <div class="order-date">${o.updatedAt || o.receivedAt}</div>
          ${o.cancellationReason ? `<div style="font-size:0.72rem;color:var(--red);margin-top:0.15rem">Reason: ${o.cancellationReason}</div>` : ''}
        </div>
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:0.4rem">
          <span class="badge badge-${o.status}">${o.status}</span>
          ${cancellable.includes(o.status) ? `<button style="font-size:0.72rem;background:#2d0707;border:1px solid var(--red);color:#fca5a5;border-radius:6px;padding:0.25rem 0.65rem;cursor:pointer;font-weight:600" onclick='openCancelModal("${o.orderID}")'>✕ Cancel</button>` : ''}
        </div>
      </div>`).join('');
  }

  function openEditModal() { document.getElementById('edit-modal').classList.add('open'); }

  async function saveProfile() {
    const body = {
      customerID: session?.customerID,
      firstName: document.getElementById('ep-first').value.trim(),
      lastName:  document.getElementById('ep-last').value.trim(),
      email:     document.getElementById('ep-email').value.trim(),
      cell:      document.getElementById('ep-cell').value.trim(),
    };
    const res = await put('/api/profile', body);
    if (res.ok) { toast('Profile updated'); closeModal('edit-modal'); loadProfile(); }
    else { toast('Update failed', true); }
  }

  // ── Products ──────────────────────────────────────────────────────────────
  function renderProducts() { showCategories(); }

  function showCategories() {
    currentCategory = null;
    document.getElementById('back-btn').style.display = 'none';
    document.getElementById('products-heading').textContent = 'Shop by Category';
    document.getElementById('products-sub').textContent = 'Select a category to browse parts';
    document.getElementById('products-grid').innerHTML = Object.entries(CATALOGUE).map(([key, cat]) => `
      <div class="product-card" onclick="showVariants('${key}')" style="cursor:pointer">
        <div class="product-icon">${cat.icon}</div>
        <div class="product-name">${cat.name}</div>
        <div class="product-desc">${cat.desc}</div>
        <div class="product-meta"><span>${cat.variants.length} options</span>
          <span>From R ${Math.min(...cat.variants.map(v=>v.price)).toLocaleString('en-ZA')}</span></div>
        <button class="btn-apply">Browse ${cat.name} →</button>
      </div>`).join('');
  }

  function showVariants(catKey) {
    currentCategory = catKey;
    const cat = CATALOGUE[catKey];
    document.getElementById('back-btn').style.display = '';
    document.getElementById('products-heading').textContent = cat.icon + '  ' + cat.name;
    document.getElementById('products-sub').textContent = cat.desc;
    document.getElementById('products-grid').innerHTML = cat.variants.map(v => {
      const outOfStock = stockMap[v.id]?.qty === 0;
      return `<div class="product-card">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:0.5rem">
          <div class="product-name" style="font-size:0.92rem">${v.name}</div>
          <div style="font-size:1.15rem;font-weight:800;color:var(--green);white-space:nowrap">R ${v.price.toLocaleString('en-ZA')}</div>
        </div>
        <div style="font-size:0.7rem;color:var(--muted);font-family:monospace;margin:0.2rem 0 0.3rem">${v.sku}</div>
        <div style="margin-bottom:0.5rem">${stockBadge(v.id)}</div>
        <div class="product-desc">${v.desc}</div>
        <button class="btn-apply" ${outOfStock ? 'disabled style="background:var(--border);color:var(--muted);cursor:not-allowed"' : ''} onclick='openApply(${JSON.stringify(v).replace(/'/g, "&#39;")})'>
          ${outOfStock ? 'Out of Stock' : 'Add to Cart'}
        </button>
      </div>`;
    }).join('');
  }

  function openApply(variant) {
    currentProduct = variant;
    document.getElementById('apply-icon').textContent  = CATALOGUE[currentCategory]?.icon || '🔧';
    document.getElementById('apply-title').textContent = variant.name;
    document.getElementById('apply-sku').textContent   = variant.sku;
    document.getElementById('apply-price').textContent = 'R ' + variant.price.toLocaleString('en-ZA');
    document.getElementById('apply-desc').textContent  = variant.desc;
    const inCart    = cart.find(i => i.variant.id === variant.id)?.qty || 0;
    const available = (stockMap[variant.id]?.qty ?? Infinity) - inCart;
    const maxQty    = Math.max(0, available);

    document.getElementById('apply-qty').value = '1';
    document.getElementById('apply-qty-select').value = '1';
    document.getElementById('apply-qty-custom').style.display = 'none';
    document.getElementById('apply-qty-custom').value = '';
    document.getElementById('apply-qty-custom').max = maxQty;

    // Show stock availability below the price
    const stockInfo = maxQty === 0
      ? '<span style="color:var(--red);font-size:0.75rem">Already at max stock in cart</span>'
      : isFinite(available)
        ? `<span style="color:var(--text2);font-size:0.75rem">${available} available</span>`
        : '';
    document.getElementById('apply-sku').innerHTML = variant.sku + (stockInfo ? '&nbsp;&nbsp;' + stockInfo : '');

    updateTotal();
    clearAlert('apply-alert');
    document.getElementById('apply-modal').classList.add('open');
  }

  // ── Cancel order ──────────────────────────────────────────────────────────
  let cancelOrderID = null;

  function openCancelModal(orderID) {
    cancelOrderID = orderID;
    document.getElementById('cancel-order-id').textContent = orderID;
    document.getElementById('cancel-reason-select').value = 'Customer changed mind';
    document.getElementById('cancel-other-field').style.display = 'none';
    clearAlert('cancel-alert');
    document.getElementById('cancel-modal').classList.add('open');
  }

  function updateCancelReason() {
    const sel = document.getElementById('cancel-reason-select').value;
    document.getElementById('cancel-other-field').style.display = sel === 'Other' ? '' : 'none';
  }

  async function submitCancel() {
    const sel    = document.getElementById('cancel-reason-select').value;
    const other  = document.getElementById('cancel-other').value.trim();
    const reason = sel === 'Other' ? (other || 'Other') : sel;
    if (!cancelOrderID) return;

    const res  = await post('/api/cancel', { orderID: cancelOrderID, reason });
    const data = await res.json();
    if (res.ok) {
      toast('Order ' + cancelOrderID + ' cancelled');
      closeModal('cancel-modal');
      loadOrders();
    } else {
      setAlert('cancel-alert', 'error', data.error || 'Cancellation failed');
    }
  }

  // ── Stock levels ──────────────────────────────────────────────────────────
  let stockMap = {};

  async function loadStock() {
    try {
      const data = await fetch('/api/stock', { credentials: 'include' }).then(r => r.json());
      stockMap = data;
    } catch (e) { stockMap = {}; }
  }

  function stockBadge(productID) {
    const s = stockMap[productID];
    if (!s) return '';
    if (s.qty === 0) return '<span style="font-size:0.7rem;background:#2d0707;color:#ef4444;border-radius:4px;padding:0.15rem 0.45rem;font-weight:700">OUT OF STOCK</span>';
    if (s.qty <= 10) return `<span style="font-size:0.7rem;background:#451a03;color:#f59e0b;border-radius:4px;padding:0.15rem 0.45rem;font-weight:700">LOW: ${s.qty} left</span>`;
    return `<span style="font-size:0.7rem;background:#052e16;color:#22c55e;border-radius:4px;padding:0.15rem 0.45rem;font-weight:700">In stock: ${s.qty}</span>`;
  }

  function onQtySelect() {
    const sel    = document.getElementById('apply-qty-select');
    const custom = document.getElementById('apply-qty-custom');
    if (sel.value === 'custom') {
      custom.style.display = '';
      custom.focus();
      document.getElementById('apply-qty').value = custom.value || '1';
    } else {
      custom.style.display = 'none';
      document.getElementById('apply-qty').value = sel.value;
    }
    updateTotal();
  }

  function updateTotal() {
    const sel    = document.getElementById('apply-qty-select');
    const custom = document.getElementById('apply-qty-custom');
    if (sel.value === 'custom') {
      document.getElementById('apply-qty').value = custom.value || '1';
    }
    const qty   = parseInt(document.getElementById('apply-qty').value) || 1;
    const total = (currentProduct?.price || 0) * qty;
    document.getElementById('apply-total-val').textContent = 'R ' + total.toLocaleString('en-ZA', {minimumFractionDigits:2});
  }

  function submitOrder() {
    const qty = parseInt(document.getElementById('apply-qty').value) || 1;
    if (!currentProduct) return;

    const available = stockMap[currentProduct.id]?.qty ?? Infinity;
    const inCart    = cart.find(i => i.variant.id === currentProduct.id)?.qty || 0;
    const canAdd    = available - inCart;

    if (qty > canAdd) {
      const msg = canAdd <= 0
        ? 'No stock available — already in your cart.'
        : `Only ${canAdd} unit${canAdd === 1 ? '' : 's'} available (${inCart} already in cart).`;
      setAlert('apply-alert', 'error', msg);
      return;
    }

    const existing = cart.find(i => i.variant.id === currentProduct.id);
    if (existing) {
      existing.qty += qty;
    } else {
      cart.push({ variant: currentProduct, qty, category: currentCategory });
    }

    updateCartBadge();
    closeModal('apply-modal');
    toast(qty + '× ' + currentProduct.name + ' added to cart');
  }

  // ── Cart ──────────────────────────────────────────────────────────────────
  function updateCartBadge() {
    const total = cart.reduce((s, i) => s + i.qty, 0);
    const badge = document.getElementById('cart-badge');
    badge.textContent = total;
    badge.style.display = total > 0 ? 'flex' : 'none';
  }

  function openCart() {
    renderCart();
    document.getElementById('cart-drawer').classList.add('open');
    document.getElementById('drawer-overlay').classList.add('open');
  }

  function closeCart() {
    document.getElementById('cart-drawer').classList.remove('open');
    document.getElementById('drawer-overlay').classList.remove('open');
  }

  function renderCart() {
    const body = document.getElementById('cart-body');
    const foot = document.getElementById('cart-foot');
    if (!cart.length) {
      body.innerHTML = '<div class="cart-empty-msg">Your cart is empty.<br>Browse the Shop to add items.</div>';
      foot.style.display = 'none';
      return;
    }
    body.innerHTML = cart.map((item, idx) => `
      <div class="cart-item">
        <div class="cart-item-info">
          <div class="cart-item-name">${item.variant.name}</div>
          <div class="cart-item-sku">${item.variant.sku}</div>
          <div class="cart-item-price">R ${(item.variant.price * item.qty).toLocaleString('en-ZA', {minimumFractionDigits:2})}
            <span style="color:var(--muted);font-weight:400;font-size:0.72rem"> (${item.qty} × R ${item.variant.price.toLocaleString('en-ZA')})</span>
          </div>
          <button class="cart-remove" onclick="removeCartItem(${idx})">✕ Remove</button>
        </div>
        <div class="cart-qty">
          <button onclick="changeQty(${idx}, -1)">−</button>
          <span>${item.qty}</span>
          <button onclick="changeQty(${idx}, +1)">+</button>
        </div>
      </div>`).join('');
    const grandTotal = cart.reduce((s, i) => s + i.variant.price * i.qty, 0);
    document.getElementById('cart-total-display').textContent =
      'R ' + grandTotal.toLocaleString('en-ZA', {minimumFractionDigits:2});
    foot.style.display = '';
  }

  function changeQty(idx, delta) {
    const maxStock = stockMap[cart[idx].variant.id]?.qty ?? Infinity;
    cart[idx].qty = Math.min(maxStock, Math.max(1, cart[idx].qty + delta));
    updateCartBadge();
    renderCart();
  }

  function removeCartItem(idx) {
    cart.splice(idx, 1);
    updateCartBadge();
    renderCart();
  }

  async function checkout() {
    if (!cart.length) return;
    const btn = document.querySelector('#cart-foot .btn-primary');
    btn.disabled = true; btn.textContent = 'Placing orders…';

    const errors = [];
    for (const item of cart) {
      const total  = item.variant.price * item.qty;
      const res    = await post('/api/order', { product: item.variant.id, amount: total, qty: item.qty });
      const data   = await res.json();
      if (!res.ok) errors.push(item.variant.name + ': ' + (data.error || 'failed'));
    }

    if (errors.length) {
      toast(errors.join('; '), true);
    } else {
      toast(cart.length + ' order' + (cart.length > 1 ? 's' : '') + ' placed successfully!');
      cart = [];
      updateCartBadge();
      closeCart();
      loadOrders();
    }

    btn.disabled = false; btn.textContent = 'Checkout →';
  }

  // ── Helpers ───────────────────────────────────────────────────────────────
  function fmt(s) { return (s||'').replace(/_/g,' '); }

  async function post(path, body) {
    return fetch(path, { method:'POST', credentials:'include', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
  }
  async function put(path, body) {
    return fetch(path, { method:'PUT', credentials:'include', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
  }

  function closeModal(id) { document.getElementById(id).classList.remove('open'); }

  function setAlert(id, type, msg) {
    const el = document.getElementById(id);
    el.textContent = msg; el.className = 'alert ' + type;
  }
  function clearAlert(id) { const el = document.getElementById(id); el.textContent=''; el.className='alert'; }

  function toast(msg, error=false) {
    const t = document.getElementById('toast');
    t.textContent = msg; t.className = 'toast show' + (error?' error':'');
    setTimeout(() => t.className='toast', 3000);
  }
</script>
</body>
</html>
""";
    }
}
