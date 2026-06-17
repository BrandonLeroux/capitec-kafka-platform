package com.capitec.kafka.orderservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OrderUiServer {

    private static final Logger log = LoggerFactory.getLogger(OrderUiServer.class);
    private final int port;
    private final OrderRepository    orderRepo;
    private final CustomerRepository customerRepo;

    public OrderUiServer(int port, OrderRepository orderRepo, CustomerRepository customerRepo) {
        this.port         = port;
        this.orderRepo    = orderRepo;
        this.customerRepo = customerRepo;
    }

    public void start() {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        Thread t = new Thread(() -> {
            try (ServerSocket server = new ServerSocket(port)) {
                log.info("Order dashboard ready at http://localhost:{}", port);
                while (!Thread.currentThread().isInterrupted()) {
                    Socket socket = server.accept();
                    pool.submit(() -> handle(socket));
                }
            } catch (Exception e) { log.error("UI server error", e); }
        });
        t.setDaemon(true);
        t.start();
    }

    private void handle(Socket socket) {
        try (socket;
             BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             OutputStream   out = socket.getOutputStream()) {

            String requestLine = in.readLine();
            if (requestLine == null) return;
            String[] parts = requestLine.split(" ");
            String method  = parts[0];
            String rawPath = parts.length > 1 ? parts[1] : "/";

            int contentLength = 0;
            String line;
            while ((line = in.readLine()) != null && !line.isEmpty()) {
                if (line.toLowerCase().startsWith("content-length:"))
                    contentLength = Integer.parseInt(line.split(":", 2)[1].trim());
            }
            String body = "";
            if (contentLength > 0) {
                char[] buf = new char[contentLength];
                in.read(buf, 0, contentLength);
                body = new String(buf);
            }

            String path  = rawPath.contains("?") ? rawPath.substring(0, rawPath.indexOf('?')) : rawPath;
            String query = rawPath.contains("?") ? rawPath.substring(rawPath.indexOf('?') + 1) : "";

            route(method, path, query, body, out);
        } catch (Exception e) { log.error("Request error", e); }
    }

    private void route(String method, String path, String query, String body, OutputStream out) throws Exception {
        // Dashboard UI
        if ("GET".equals(method) && "/".equals(path)) {
            respond(out, 200, "text/html; charset=UTF-8", buildHtml());

        // Orders API
        } else if ("GET".equals(method) && "/api/orders".equals(path)) {
            respond(out, 200, "application/json", ordersJson(query));

        // Seed endpoint — direct DB upsert without Kafka (used by create-orders.sh)
        } else if ("POST".equals(method) && "/api/order/seed".equals(path)) {
            Order o = new Order();
            o.orderID    = JsonParser.getString(body, "orderID");
            o.customerID = JsonParser.getString(body, "customerID");
            o.product    = JsonParser.getString(body, "product");
            o.amount     = JsonParser.getDouble(body, "amount");
            o.status     = JsonParser.getString(body, "status");
            if (o.orderID == null || o.customerID == null) {
                respond(out, 400, "application/json", "{\"error\":\"orderID and customerID required\"}");
            } else {
                orderRepo.upsert(o);
                respond(out, 200, "application/json", "{\"orderID\":\"" + esc(o.orderID) + "\"}");
            }

        // Customers API
        } else if ("GET".equals(method) && "/api/customers".equals(path)) {
            respond(out, 200, "application/json", customersJson(query));

        // Customer by cell (used by customer-portal for login)
        } else if ("GET".equals(method) && "/api/customer/by-cell".equals(path)) {
            String cell = param(query, "cell");
            if (cell == null) { respond(out, 400, "application/json", "{\"error\":\"cell required\"}"); return; }
            Customer c = customerRepo.findByCell(cell);
            if (c == null) { respond(out, 404, "application/json", "{\"error\":\"not found\"}"); return; }
            respond(out, 200, "application/json", customerToJson(c));

        // Customer by ID or max-number
        } else if ("GET".equals(method) && path.startsWith("/api/customer/")) {
            String id = path.substring("/api/customer/".length());
            if ("max-number".equals(id)) {
                long max = customerRepo.maxCustomerNumber();
                respond(out, 200, "application/json", "{\"maxCustomerNumber\":" + max + "}");
            } else {
                Customer c = customerRepo.findById(id);
                if (c == null) { respond(out, 404, "application/json", "{\"error\":\"not found\"}"); return; }
                respond(out, 200, "application/json", customerToJson(c));
            }

        // Register / upsert customer (called by customer-portal on update)
        } else if ("POST".equals(method) && "/api/customer/register".equals(path)) {
            Customer c = parseCustomerFromJson(body);
            if (c == null || c.customerID == null) { respond(out, 400, "application/json", "{\"error\":\"invalid body\"}"); return; }
            customerRepo.upsert(c);
            respond(out, 200, "application/json", "{\"customerID\":\"" + esc(c.customerID) + "\"}");

        // Inventory proxy — forwards to inventory-service
        } else if ("GET".equals(method) && path.startsWith("/api/inventory")) {
            String inventoryUrl = "http://inventory-service:8083" + path + (query.isBlank() ? "" : "?" + query);
            String result = httpGet(inventoryUrl);
            respond(out, 200, "application/json", result != null ? result : "{\"total\":0,\"items\":[]}");

        } else {
            respond(out, 404, "application/json", "{\"error\":\"not found\"}");
        }
    }

    // ── API implementations ───────────────────────────────────────────────────

    private String ordersJson(String query) throws Exception {
        String search     = param(query, "search");
        String status     = param(query, "status");
        String customerID = param(query, "customerID");
        int page          = intParam(query, "page", 1);
        int pageSize      = intParam(query, "size", 20);
        int offset        = (page - 1) * pageSize;

        List<Order> orders = orderRepo.findAll(search, status, customerID, pageSize, offset);
        int total = orderRepo.count(search, status, customerID);

        StringBuilder sb = new StringBuilder();
        sb.append("{\"total\":").append(total)
          .append(",\"page\":").append(page)
          .append(",\"pageSize\":").append(pageSize)
          .append(",\"stats\":{")
          .append("\"PENDING\":").append(orderRepo.countByStatus("PENDING"))
          .append(",\"CONFIRMED\":").append(orderRepo.countByStatus("CONFIRMED"))
          .append(",\"PAYMENT-INIT\":").append(orderRepo.countByStatus("PAYMENT-INIT"))
          .append(",\"PAYMENT-PROCESSED\":").append(orderRepo.countByStatus("PAYMENT-PROCESSED"))
          .append(",\"PACKED\":").append(orderRepo.countByStatus("PACKED"))
          .append(",\"OUT-FOR-DELIVERY\":").append(orderRepo.countByStatus("OUT-FOR-DELIVERY"))
          .append(",\"DELIVERED\":").append(orderRepo.countByStatus("DELIVERED"))
          .append(",\"CANCELLED\":").append(orderRepo.countByStatus("CANCELLED"))
          .append("},\"orders\":[");
        for (int i = 0; i < orders.size(); i++) {
            Order o = orders.get(i);
            if (i > 0) sb.append(",");
            sb.append(String.format(
                "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"product\":\"%s\",\"amount\":%.2f,\"status\":\"%s\",\"cancellationReason\":\"%s\",\"receivedAt\":\"%s\",\"updatedAt\":\"%s\"}",
                esc(o.orderID), esc(o.customerID), esc(o.product), o.amount,
                esc(o.status), esc(o.cancellationReason), esc(o.receivedAt), esc(o.updatedAt)));
        }
        sb.append("]}");
        return sb.toString();
    }

    private String customersJson(String query) throws Exception {
        String search = param(query, "search");
        int page      = intParam(query, "page", 1);
        int pageSize  = intParam(query, "size", 20);
        int offset    = (page - 1) * pageSize;
        List<Customer> customers = customerRepo.findAll(search, pageSize, offset);
        int total = customerRepo.count(search);
        StringBuilder sb = new StringBuilder();
        sb.append("{\"total\":").append(total).append(",\"customers\":[");
        for (int i = 0; i < customers.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(customerToJson(customers.get(i)));
        }
        sb.append("]}");
        return sb.toString();
    }

    private String customerToJson(Customer c) {
        return String.format(
            "{\"customerId\":\"%s\",\"customerID\":\"%s\",\"customerNumber\":%d,\"firstName\":\"%s\",\"lastName\":\"%s\",\"idNumber\":\"%s\",\"email\":\"%s\",\"cell\":\"%s\",\"passwordHash\":\"%s\",\"password_hash\":\"%s\",\"createdAt\":\"%s\"}",
            esc(c.customerID), esc(c.customerID), c.customerNumber,
            esc(c.firstName), esc(c.lastName), esc(c.idNumber),
            esc(c.email), esc(c.cell), esc(c.passwordHash), esc(c.passwordHash), esc(c.createdAt));
    }

    private Customer parseCustomerFromJson(String json) {
        if (json == null || json.isBlank()) return null;
        Customer c = new Customer();
        c.customerID   = JsonParser.getString(json, "customerID");
        c.firstName    = JsonParser.getString(json, "firstName");
        c.lastName     = JsonParser.getString(json, "lastName");
        c.idNumber     = JsonParser.getString(json, "idNumber");
        c.email        = JsonParser.getString(json, "email");
        c.cell         = JsonParser.getString(json, "cell");
        c.passwordHash = JsonParser.getString(json, "passwordHash");
        c.customerNumber = JsonParser.getLong(json, "customerNumber");
        return c;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    private String param(String query, String name) throws UnsupportedEncodingException {
        if (query == null || query.isBlank()) return null;
        for (String part : query.split("&")) {
            String[] kv = part.split("=", 2);
            if (kv.length == 2 && kv[0].equals(name)) return URLDecoder.decode(kv[1], "UTF-8");
        }
        return null;
    }

    private int intParam(String query, String name, int def) throws UnsupportedEncodingException {
        String v = param(query, name);
        try { return v != null ? Integer.parseInt(v) : def; } catch (NumberFormatException e) { return def; }
    }

    private String httpGet(String urlStr) {
        try {
            java.net.HttpURLConnection c = (java.net.HttpURLConnection) new java.net.URL(urlStr).openConnection();
            c.setConnectTimeout(3000); c.setReadTimeout(5000);
            if (c.getResponseCode() == 200)
                return new String(c.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) { log.warn("httpGet failed url={} err={}", urlStr, e.getMessage()); }
        return null;
    }

    private String esc(String s) {
        if (s == null) return "";
        return s.replace("\\","\\\\").replace("\"","\\\"");
    }

    private void respond(OutputStream out, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        String headers = "HTTP/1.1 " + status + " OK\r\n" +
            "Content-Type: " + contentType + "\r\n" +
            "Content-Length: " + bytes.length + "\r\n" +
            "Connection: close\r\n\r\n";
        out.write(headers.getBytes(StandardCharsets.UTF_8));
        out.write(bytes);
        out.flush();
    }

    // ── Dashboard HTML ────────────────────────────────────────────────────────
    private String buildHtml() {
        return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Capitec Admin Dashboard</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; padding: 2rem; }
    h1 { font-size: 1.4rem; font-weight: 700; margin-bottom: 1.5rem; }
    .tabs { display: flex; gap: 0; margin-bottom: 2rem; border-bottom: 1px solid #334155; }
    .tab { padding: 0.6rem 1.4rem; font-size: 0.85rem; font-weight: 600; cursor: pointer; color: #64748b; border-bottom: 2px solid transparent; margin-bottom: -1px; transition: all .15s; }
    .tab.active { color: #818cf8; border-bottom-color: #6366f1; }
    .tab:hover:not(.active) { color: #e2e8f0; }
    .tab-panel { display: none; } .tab-panel.active { display: block; }
    .stats { display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 2rem; }
    .stat { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 1rem 1.5rem; min-width: 120px; }
    .stat-label { font-size: 0.68rem; color: #64748b; text-transform: uppercase; letter-spacing: .06em; margin-bottom: 0.3rem; }
    .stat-value { font-size: 1.5rem; font-weight: 700; }
    .toolbar { display: flex; gap: 0.75rem; margin-bottom: 1rem; flex-wrap: wrap; align-items: center; }
    input[type=text] { background: #1e293b; border: 1px solid #334155; border-radius: 8px; color: #e2e8f0; padding: 0.5rem 0.75rem; font-size: 0.85rem; outline: none; width: 260px; }
    input[type=text]:focus { border-color: #6366f1; }
    select { background: #1e293b; border: 1px solid #334155; border-radius: 8px; color: #e2e8f0; padding: 0.5rem 0.75rem; font-size: 0.85rem; outline: none; cursor: pointer; }
    .btn { background: #6366f1; color: #fff; border: none; border-radius: 8px; padding: 0.5rem 1rem; font-size: 0.85rem; font-weight: 600; cursor: pointer; }
    .btn:hover { background: #4f46e5; }
    table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
    thead th { text-align: left; padding: 0.6rem 0.75rem; color: #64748b; font-weight: 600; font-size: 0.72rem; text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid #334155; }
    .stock-ok   { color: #22c55e; font-weight: 700; }
    .stock-low  { color: #f59e0b; font-weight: 700; }
    .stock-out  { color: #ef4444; font-weight: 700; }
    tbody tr { border-bottom: 1px solid #1e293b; transition: background .1s; }
    tbody tr:hover { background: #1e293b; }
    td { padding: 0.6rem 0.75rem; color: #cbd5e1; vertical-align: middle; }
    td.mono { font-family: monospace; color: #e2e8f0; font-weight: 600; }
    .badge { display: inline-block; padding: 0.15rem 0.55rem; border-radius: 999px; font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; }
    .badge-PENDING          { background: #451a03; color: #f59e0b; }
    .badge-CONFIRMED        { background: #1e1b4b; color: #818cf8; }
    .badge-PAYMENT-INIT     { background: #052e16; color: #22c55e; }
    .badge-PAYMENT-PROCESSED{ background: #0c1a2e; color: #38bdf8; }
    .badge-PACKED           { background: #042f2e; color: #06b6d4; }
    .badge-OUT-FOR-DELIVERY { background: #431407; color: #f97316; }
    .badge-DELIVERED        { background: #1e293b; color: #94a3b8; }
    .badge-CANCELLED        { background: #2d0707; color: #ef4444; }
    .badge-UNKNOWN          { background: #1e293b; color: #475569; }
    .pagination { display: flex; gap: 0.5rem; align-items: center; margin-top: 1rem; font-size: 0.82rem; color: #64748b; }
    .pagination button { background: #1e293b; border: 1px solid #334155; color: #e2e8f0; border-radius: 6px; padding: 0.3rem 0.65rem; cursor: pointer; font-size: 0.8rem; }
    .pagination button:disabled { opacity: 0.4; cursor: default; }
    .empty { text-align: center; padding: 3rem; color: #475569; }
    .auto-badge { font-size: 0.65rem; background: #1e293b; border: 1px solid #334155; border-radius: 4px; padding: 0.1rem 0.4rem; color: #64748b; margin-left: 0.5rem; cursor: pointer; }
  </style>
</head>
<body>
<h1>Admin Dashboard <span class="auto-badge" id="auto-label">auto-refresh: ON</span></h1>
<div class="tabs">
  <div class="tab active" onclick="switchTab('orders')">Orders</div>
  <div class="tab"        onclick="switchTab('customers')">Customers</div>
  <div class="tab"        onclick="switchTab('inventory')">Inventory</div>
</div>

<!-- ORDERS -->
<div class="tab-panel active" id="tab-orders">
  <div class="stats" id="order-stats"></div>
  <div class="toolbar">
    <input type="text" id="order-search" placeholder="Search order, customer, product…" oninput="debounce(()=>{orderPage=1;loadOrders();})"/>
    <select id="status-filter" onchange="orderPage=1;loadOrders()">
      <option value="ALL">All statuses</option>
      <option value="CONFIRMED">Confirmed</option>
      <option value="PAYMENT-INIT">Payment Init</option>
      <option value="PAYMENT-PROCESSED">Payment Processed</option>
      <option value="PACKED">Packed</option>
      <option value="OUT-FOR-DELIVERY">Out for Delivery</option>
      <option value="DELIVERED">Delivered</option>
      <option value="CANCELLED">Cancelled</option>
    </select>
    <button class="btn" onclick="loadOrders()">Refresh</button>
  </div>
  <table>
    <thead><tr><th>Order ID</th><th>Customer</th><th>Product</th><th>Amount</th><th>Status</th><th>Cancellation Reason</th><th>Received</th><th>Updated</th></tr></thead>
    <tbody id="order-tbody"></tbody>
  </table>
  <div class="empty" id="order-empty" style="display:none">No orders found.</div>
  <div class="pagination" id="order-pagination"></div>
</div>

<!-- INVENTORY -->
<div class="tab-panel" id="tab-inventory">
  <div class="toolbar">
    <input type="text" id="inv-search" placeholder="Search SKU, name, product…" oninput="debounce(()=>{loadInventory();})"/>
    <select id="inv-cat" onchange="loadInventory()">
      <option value="ALL">All categories</option>
      <option>Tyres</option><option>Brakes</option><option>Batteries</option>
      <option>Filters</option><option>Wipers</option><option>Shocks</option>
      <option>Lighting</option><option>Oils</option>
    </select>
    <button class="btn" onclick="loadInventory()">Refresh</button>
  </div>
  <div id="inv-stats" style="display:flex;gap:1rem;margin-bottom:1rem;flex-wrap:wrap"></div>
  <table>
    <thead><tr>
      <th>SKU</th><th>Product ID</th><th>Name</th><th>Category</th>
      <th>Stock</th><th>Reorder Level</th><th>Unit Price</th><th>Updated</th>
    </tr></thead>
    <tbody id="inv-tbody"></tbody>
  </table>
  <div class="empty" id="inv-empty" style="display:none">No inventory items found.</div>
</div>

<!-- CUSTOMERS -->
<div class="tab-panel" id="tab-customers">
  <div class="toolbar">
    <input type="text" id="cust-search" placeholder="Search name, email, cell, ID…" oninput="debounce(()=>{custPage=1;loadCustomers();})"/>
    <button class="btn" onclick="loadCustomers()">Refresh</button>
  </div>
  <table>
    <thead><tr><th>Customer #</th><th>Customer ID</th><th>Name</th><th>ID Number</th><th>Email</th><th>Cell</th><th>Registered</th></tr></thead>
    <tbody id="cust-tbody"></tbody>
  </table>
  <div class="empty" id="cust-empty" style="display:none">No customers found.</div>
  <div class="pagination" id="cust-pagination"></div>
</div>

<script>
  let orderPage=1, custPage=1;
  const pageSize=20;
  let autoRefresh=true, autoTimer=null, debTimer=null, activeTab='orders';

  function switchTab(name) {
    activeTab=name;
    document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',['orders','customers','inventory'][i]===name));
    document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
    document.getElementById('tab-'+name).classList.add('active');
    if(name==='orders') loadOrders();
    else if(name==='inventory') loadInventory();
    else loadCustomers();
  }

  async function loadOrders() {
    const s=document.getElementById('order-search').value.trim();
    const st=document.getElementById('status-filter').value;
    const p=new URLSearchParams({page:orderPage,size:pageSize});
    if(s) p.set('search',s);
    if(st!=='ALL') p.set('status',st);
    const data=await fetch('/api/orders?'+p).then(r=>r.json());
    renderOrderStats(data.stats,data.total);
    const empty=document.getElementById('order-empty');
    if(!data.orders.length){document.getElementById('order-tbody').innerHTML='';empty.style.display='';return;}
    empty.style.display='none';
    document.getElementById('order-tbody').innerHTML=data.orders.map(o=>`
      <tr><td class="mono">${o.orderID}</td><td>${o.customerID}</td>
      <td>${fmtProduct(o.product)}</td>
      <td>R ${Number(o.amount).toLocaleString('en-ZA',{minimumFractionDigits:2})}</td>
      <td><span class="badge badge-${o.status || 'UNKNOWN'}">${o.status || '—'}</span></td>
      <td style="color:var(--text2);font-size:0.78rem">${o.cancellationReason || '—'}</td>
      <td>${o.receivedAt}</td><td>${o.updatedAt}</td></tr>`).join('');
    renderPag('order',data.total,orderPage,p=>{orderPage=p;loadOrders();});
  }

  function renderOrderStats(s,total) {
    const entries=[['','Total',total],['','CONFIRMED',s['CONFIRMED']||0],
      ['','PAYMENT-INIT',s['PAYMENT-INIT']||0],['','PAYMENT-PROCESSED',s['PAYMENT-PROCESSED']||0],
      ['','PACKED',s['PACKED']||0],['','OUT-FOR-DELIVERY',s['OUT-FOR-DELIVERY']||0],
      ['','DELIVERED',s['DELIVERED']||0],['','CANCELLED',s['CANCELLED']||0]];
    document.getElementById('order-stats').innerHTML=entries.map(([,l,v])=>
      `<div class="stat"><div class="stat-label">${l}</div><div class="stat-value">${v}</div></div>`).join('');
  }

  async function loadCustomers() {
    const s=document.getElementById('cust-search').value.trim();
    const p=new URLSearchParams({page:custPage,size:pageSize});
    if(s) p.set('search',s);
    const data=await fetch('/api/customers?'+p).then(r=>r.json());
    const empty=document.getElementById('cust-empty');
    if(!data.customers.length){document.getElementById('cust-tbody').innerHTML='';empty.style.display='';return;}
    empty.style.display='none';
    document.getElementById('cust-tbody').innerHTML=data.customers.map(c=>`
      <tr><td class="mono">${c.customerNumber > 0 ? c.customerNumber : '—'}</td><td class="mono">${c.customerID}</td>
      <td>${c.firstName} ${c.lastName}</td><td>${c.idNumber||'—'}</td>
      <td>${c.email||'—'}</td><td>${c.cell||'—'}</td><td>${c.createdAt}</td></tr>`).join('');
    renderPag('cust',data.total,custPage,p=>{custPage=p;loadCustomers();});
  }

  function renderPag(prefix,total,page,goFn) {
    const pages=Math.ceil(total/pageSize);
    const el=document.getElementById(prefix+'-pagination');
    if(pages<=1){el.innerHTML='';return;}
    el.innerHTML=`<button onclick="go_${prefix}(${page-1})" ${page===1?'disabled':''}>← Prev</button>
      <span>Page ${page} of ${pages} (${total} rows)</span>
      <button onclick="go_${prefix}(${page+1})" ${page>=pages?'disabled':''}>Next →</button>`;
  }

  window.go_order=p=>{orderPage=p;loadOrders();};
  window.go_cust =p=>{custPage=p;loadCustomers();};

  function fmtProduct(s) {
    if (!s) return '—';
    // TYRE_175_65_R14 → Tyre 175/65 R14  |  BRAKE_PAD_FRONT_STD → Brake Pad Front Std
    return s.replace(/_/g,' ').split(' ').map(w=>w.charAt(0).toUpperCase()+w.slice(1).toLowerCase()).join(' ')
            .replace(/(\\d+) (\\d+) (R\\d+)/i,'$1/$2 $3');
  }

  function debounce(fn){clearTimeout(debTimer);debTimer=setTimeout(fn,300);}

  async function loadInventory() {
    const search = document.getElementById('inv-search').value.trim();
    const cat    = document.getElementById('inv-cat').value;
    const p = new URLSearchParams({ size: 200 });
    if (search) p.set('search', search);
    if (cat !== 'ALL') p.set('category', cat);
    const data = await fetch('/api/inventory?' + p).then(r => r.json()).catch(() => ({ total:0, lowStock:0, items:[] }));

    document.getElementById('inv-stats').innerHTML = [
      ['total','Total SKUs', data.total || 0],
      ['low','Low Stock',    data.lowStock || 0],
    ].map(([,l,v]) => `<div class="stat"><div class="stat-label">${l}</div><div class="stat-value">${v}</div></div>`).join('');

    const items = data.items || [];
    const empty = document.getElementById('inv-empty');
    if (!items.length) { document.getElementById('inv-tbody').innerHTML = ''; empty.style.display = ''; return; }
    empty.style.display = 'none';
    document.getElementById('inv-tbody').innerHTML = items.map(i => {
      const cls = i.quantity === 0 ? 'stock-out' : i.quantity <= i.reorderLevel ? 'stock-low' : 'stock-ok';
      return `<tr>
        <td class="mono">${i.sku}</td>
        <td style="font-size:0.75rem;color:#64748b">${i.productID}</td>
        <td>${i.name}</td><td>${i.category}</td>
        <td class="${cls}">${i.quantity}</td>
        <td>${i.reorderLevel}</td>
        <td>R ${Number(i.unitPrice).toLocaleString('en-ZA',{minimumFractionDigits:2})}</td>
        <td>${i.updatedAt}</td>
      </tr>`;
    }).join('');
  }

  function scheduleAuto(){
    clearTimeout(autoTimer);
    if(!autoRefresh) return;
    autoTimer=setTimeout(()=>{
      if(activeTab==='orders') loadOrders();
      else if(activeTab==='inventory') loadInventory();
      else loadCustomers();
      scheduleAuto();
    },3000);
  }

  document.getElementById('auto-label').addEventListener('click',()=>{
    autoRefresh=!autoRefresh;
    document.getElementById('auto-label').textContent='auto-refresh: '+(autoRefresh?'ON':'OFF');
    scheduleAuto();
  });

  loadOrders();
  scheduleAuto();
</script>
</body>
</html>
""";
    }
}
