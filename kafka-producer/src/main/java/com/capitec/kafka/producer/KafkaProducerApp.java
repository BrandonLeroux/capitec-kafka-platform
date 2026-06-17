package com.capitec.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaProducerApp {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerApp.class);
    private static final int PORT = 8080;

    public static void main(String[] args) throws Exception {
        Properties props    = loadConfig();
        String orderTopic   = (String) props.remove("topic");
        String customerTopic = (String) props.remove("customer.topic");
        if (orderTopic    == null) throw new IllegalStateException("'topic' must be set in producer.properties");
        if (customerTopic == null) throw new IllegalStateException("'customer.topic' must be set in producer.properties");

        KafkaProducer<String, String> producer = new KafkaProducer<>(buildProducerProperties(
                props.getProperty("bootstrap.servers")));

        ExecutorService pool = Executors.newFixedThreadPool(4);
        try (ServerSocket server = new ServerSocket(PORT)) {
            log.info("Producer UI ready at http://localhost:{}", PORT);
            while (true) {
                Socket socket = server.accept();
                pool.submit(() -> handleRequest(socket, producer, orderTopic, customerTopic));
            }
        }
    }

    private static void handleRequest(Socket socket, KafkaProducer<String, String> producer,
                                      String orderTopic, String customerTopic) {
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

            String path = rawPath.contains("?") ? rawPath.substring(0, rawPath.indexOf('?')) : rawPath;

            if      ("GET".equals(method)  && "/".equals(path))               sendHtml(out, buildHtml());
            else if ("POST".equals(method) && "/send".equals(path))            handleSend(out, body, producer, orderTopic);
            else if ("POST".equals(method) && "/register".equals(path))        handleRegister(out, body, producer, customerTopic);
            else respond(out, 404, "application/json", "{\"error\":\"not found\"}");

        } catch (Exception e) {
            log.error("Request error", e);
        }
    }

    // ── Send order ────────────────────────────────────────────────────────────
    private static void handleSend(OutputStream out, String body,
                                   KafkaProducer<String, String> producer, String topic) throws IOException {
        String key   = extractJson(body, "key");
        String value = extractJson(body, "value");

        if (value == null || value.isBlank()) {
            respond(out, 400, "application/json", "{\"error\":\"value is required\"}"); return;
        }
        if (key == null || key.isBlank()) key = "key-" + System.currentTimeMillis();

        final String finalKey = key;
        try {
            RecordMetadata meta = producer.send(new ProducerRecord<>(topic, finalKey, value)).get();
            log.info("Order sent key={} topic={} partition={} offset={}", finalKey, meta.topic(), meta.partition(), meta.offset());
            respond(out, 200, "application/json", String.format(
                "{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":\"%s\"}",
                meta.topic(), meta.partition(), meta.offset(), finalKey));
        } catch (Exception e) {
            log.error("Failed to send order", e);
            respond(out, 500, "application/json", "{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    // ── Register / login customer ─────────────────────────────────────────────
    private static void handleRegister(OutputStream out, String body,
                                       KafkaProducer<String, String> producer, String topic) throws IOException {
        String customerID = extractJson(body, "customerID");
        String firstName  = extractJson(body, "firstName");
        String lastName   = extractJson(body, "lastName");
        String email      = extractJson(body, "email");
        String cell       = extractJson(body, "cell");

        // If no customerID supplied → generate a new one (new registration)
        boolean isNew = (customerID == null || customerID.isBlank());
        if (isNew) customerID = "CUST-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        if (!isNew) {
            // Login — just return the customerID, no Kafka message needed
            respond(out, 200, "application/json",
                String.format("{\"customerID\":\"%s\",\"isNew\":false}", customerID));
            return;
        }

        // New registration — publish to customer-created topic
        String payload = String.format(
            "{\"customerID\":\"%s\",\"firstName\":\"%s\",\"lastName\":\"%s\",\"email\":\"%s\",\"cell\":\"%s\"}",
            customerID,
            firstName  != null ? firstName  : "",
            lastName   != null ? lastName   : "",
            email      != null ? email      : "",
            cell       != null ? cell       : "");

        final String finalCID = customerID;
        try {
            producer.send(new ProducerRecord<>(topic, finalCID, payload)).get();
            log.info("Customer registered customerID={} name={} {}", finalCID, firstName, lastName);
            respond(out, 200, "application/json",
                String.format("{\"customerID\":\"%s\",\"isNew\":true}", finalCID));
        } catch (Exception e) {
            log.error("Failed to publish customer", e);
            respond(out, 500, "application/json", "{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    // ── Kafka producer config ─────────────────────────────────────────────────
    private static Properties buildProducerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,                bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,             StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,           StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG,                             "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,               true);
        props.put(ProducerConfig.RETRIES_CONFIG,                          Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,              120_000);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,                 100);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,   5);
        props.put(ProducerConfig.LINGER_MS_CONFIG,                        5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,                       32_768);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,                 "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,                    33_554_432L);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,                     5_000);
        return props;
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────
    private static void sendHtml(OutputStream out, String html) throws IOException {
        respond(out, 200, "text/html; charset=UTF-8", html);
    }

    private static void respond(OutputStream out, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        String headers = "HTTP/1.1 " + status + " OK\r\n" +
                "Content-Type: " + contentType + "\r\n" +
                "Content-Length: " + bytes.length + "\r\n" +
                "Connection: close\r\n\r\n";
        out.write(headers.getBytes(StandardCharsets.UTF_8));
        out.write(bytes);
        out.flush();
    }

    private static String extractJson(String json, String field) {
        String search = "\"" + field + "\"";
        int idx = json.indexOf(search);
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + search.length());
        if (colon < 0) return null;
        int start = json.indexOf('"', colon + 1);
        if (start < 0) return null;
        int end = start + 1;
        while (end < json.length()) {
            if (json.charAt(end) == '"' && json.charAt(end - 1) != '\\') break;
            end++;
        }
        return json.substring(start + 1, end).replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private static Properties loadConfig() throws IOException {
        try (InputStream in = KafkaProducerApp.class.getResourceAsStream("/producer.properties")) {
            if (in == null) throw new IOException("producer.properties not found on classpath");
            Properties props = new Properties();
            props.load(in);
            String override = System.getProperty("bootstrap.servers");
            if (override != null && !override.isBlank()) props.put("bootstrap.servers", override);
            return props;
        }
    }

    // ── HTML ──────────────────────────────────────────────────────────────────
    private static String buildHtml() {
        return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Kafka Producer</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 2rem; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 2rem; width: 100%; max-width: 520px; }
    h2 { font-size: 1.1rem; font-weight: 700; margin-bottom: 1.25rem; }
    label { display: block; font-size: 0.78rem; color: #94a3b8; margin-bottom: 0.35rem; margin-top: 0.9rem; }
    input, textarea, select { width: 100%; background: #0f172a; border: 1px solid #334155; border-radius: 8px; color: #e2e8f0; padding: 0.55rem 0.75rem; font-size: 0.88rem; font-family: inherit; outline: none; transition: border-color .15s; }
    input:focus, textarea:focus, select:focus { border-color: #6366f1; }
    textarea { resize: vertical; min-height: 110px; font-family: 'Menlo','Consolas',monospace; font-size: 0.8rem; }
    .row { display: flex; gap: 0.75rem; margin-top: 1rem; }
    .btn-primary { flex:1; background:#6366f1; color:#fff; border:none; border-radius:8px; padding:0.6rem 1rem; font-size:0.88rem; font-weight:600; cursor:pointer; }
    .btn-primary:hover { background:#4f46e5; }
    .btn-primary:disabled { background:#334155; color:#64748b; cursor:not-allowed; }
    .btn-ghost { flex:0 0 auto; background:#1e293b; border:1px solid #334155; color:#94a3b8; border-radius:8px; padding:0.6rem 1rem; font-size:0.88rem; cursor:pointer; }
    .btn-ghost:hover { background:#0f172a; color:#e2e8f0; }
    .customer-bar { display:flex; align-items:center; gap:0.75rem; background:#0f172a; border:1px solid #334155; border-radius:8px; padding:0.5rem 0.75rem; margin-bottom:1.5rem; font-size:0.8rem; }
    .customer-bar .cid { font-family:monospace; color:#818cf8; font-weight:700; }
    .customer-bar .logout { margin-left:auto; cursor:pointer; color:#64748b; font-size:0.75rem; }
    .customer-bar .logout:hover { color:#ef4444; }
    .log { margin-top:1.25rem; }
    .log h3 { font-size:0.72rem; color:#64748b; text-transform:uppercase; letter-spacing:.05em; margin-bottom:0.4rem; }
    .log-list { list-style:none; display:flex; flex-direction:column; gap:0.35rem; max-height:180px; overflow-y:auto; }
    .log-item { background:#0f172a; border:1px solid #1e293b; border-radius:6px; padding:0.4rem 0.65rem; font-size:0.75rem; font-family:monospace; display:flex; gap:0.5rem; align-items:flex-start; }
    .log-item.ok  { border-left:3px solid #22c55e; }
    .log-item.err { border-left:3px solid #ef4444; }
    .log-meta { color:#64748b; white-space:nowrap; }
    .log-body { color:#cbd5e1; word-break:break-all; }
    .toast { position:fixed; bottom:1.5rem; right:1.5rem; background:#22c55e; color:#fff; padding:0.55rem 1rem; border-radius:8px; font-size:0.82rem; font-weight:600; opacity:0; pointer-events:none; transition:opacity .3s; }
    .toast.show { opacity:1; }
    .toast.error { background:#ef4444; }
    .divider { border:none; border-top:1px solid #334155; margin:1.25rem 0; }
    .hint { font-size:0.72rem; color:#475569; margin-top:0.4rem; }
  </style>
</head>
<body>
<div class="card" id="login-card">
  <h2>Sign in</h2>
  <p class="hint" style="margin-top:0">Enter your customer number to sign in, or leave blank to register.</p>

  <label>Customer Number (leave blank to register)</label>
  <input id="login-cid" type="text" placeholder="CUST-XXXXXXXX"/>

  <div id="register-fields" style="display:none">
    <hr class="divider"/>
    <label>First Name</label><input id="reg-first" type="text" placeholder="Jane"/>
    <label>Last Name</label><input id="reg-last"  type="text" placeholder="Smith"/>
    <label>Email</label>    <input id="reg-email" type="email" placeholder="jane@example.com"/>
    <label>Cell</label>     <input id="reg-cell"  type="tel"   placeholder="082 000 0000"/>
  </div>

  <div class="row" style="margin-top:1.25rem">
    <button class="btn-primary" id="login-btn" onclick="handleLogin()">Continue</button>
  </div>
</div>

<div class="card" id="producer-card" style="display:none">
  <div class="customer-bar">
    <span>Logged in as</span>
    <span class="cid" id="display-cid"></span>
    <span class="logout" onclick="logout()">Sign out</span>
  </div>

  <h2>Submit Order</h2>

  <label>Product</label>
  <select id="product">
    <option value="SAVINGS_ACCOUNT">Savings Account</option>
    <option value="PERSONAL_LOAN">Personal Loan</option>
    <option value="HOME_LOAN">Home Loan</option>
    <option value="CREDIT_CARD">Credit Card</option>
    <option value="VEHICLE_FINANCE">Vehicle Finance</option>
  </select>

  <label>Amount (R)</label>
  <input id="amount" type="number" step="0.01" placeholder="1500.00"/>

  <label>Status</label>
  <select id="status">
    <option value="PENDING">Pending</option>
    <option value="CONFIRMED">Confirmed (triggers payment instruction)</option>
    <option value="SHIPPED">Shipped</option>
    <option value="DELIVERED">Delivered</option>
    <option value="CANCELLED">Cancelled</option>
  </select>

  <label>Custom payload <span style="color:#475569">(optional — overrides fields above)</span></label>
  <textarea id="custom-value" placeholder='Leave blank to auto-build from fields above'></textarea>

  <div class="row">
    <button class="btn-primary" id="send-btn" onclick="sendOrder()">Send Order</button>
    <button class="btn-ghost" onclick="clearLog()">Clear log</button>
  </div>

  <div class="log">
    <h3>Message log</h3>
    <ul class="log-list" id="log"></ul>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
  let customerID = null;
  let orderSeq   = 1;

  // ── Login / register ──────────────────────────────────────────────────────
  const loginInput = document.getElementById('login-cid');
  loginInput.addEventListener('input', () => {
    const blank = !loginInput.value.trim();
    document.getElementById('register-fields').style.display = blank ? '' : 'none';
    document.getElementById('login-btn').textContent = blank ? 'Register' : 'Sign in';
  });

  async function handleLogin() {
    const cid  = loginInput.value.trim();
    const btn  = document.getElementById('login-btn');
    btn.disabled = true;

    const payload = cid
      ? { customerID: cid }
      : {
          firstName: document.getElementById('reg-first').value.trim(),
          lastName:  document.getElementById('reg-last').value.trim(),
          email:     document.getElementById('reg-email').value.trim(),
          cell:      document.getElementById('reg-cell').value.trim(),
        };

    try {
      const res  = await fetch('/register', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
      const data = await res.json();
      if (!res.ok) { showToast(data.error || 'Error', true); return; }
      customerID = data.customerID;
      document.getElementById('display-cid').textContent = customerID;
      document.getElementById('login-card').style.display    = 'none';
      document.getElementById('producer-card').style.display = '';
      showToast(data.isNew ? 'Registered as ' + customerID : 'Welcome back, ' + customerID);
    } finally {
      btn.disabled = false;
    }
  }

  function logout() {
    customerID = null;
    loginInput.value = '';
    document.getElementById('register-fields').style.display = 'none';
    document.getElementById('login-btn').textContent = 'Continue';
    document.getElementById('login-card').style.display    = '';
    document.getElementById('producer-card').style.display = 'none';
  }

  // ── Send order ────────────────────────────────────────────────────────────
  async function sendOrder() {
    const custom  = document.getElementById('custom-value').value.trim();
    const product = document.getElementById('product').value;
    const amount  = parseFloat(document.getElementById('amount').value || '0');
    const status  = document.getElementById('status').value;

    const orderID = 'ORD-' + customerID + '-' + (orderSeq++).toString().padStart(3,'0');
    const value   = custom || JSON.stringify({ orderID, customerID, product, amount, status });

    const btn = document.getElementById('send-btn');
    btn.disabled = true; btn.textContent = 'Sending…';

    try {
      const res  = await fetch('/send', { method:'POST', headers:{'Content-Type':'application/json'},
                                          body: JSON.stringify({ key: orderID, value }) });
      const data = await res.json();
      if (res.ok) {
        addLog(true, `partition=${data.partition} offset=${data.offset} key=${data.key}`, value);
        showToast('Sent → partition ' + data.partition + ', offset ' + data.offset);
        document.getElementById('custom-value').value = '';
        document.getElementById('amount').value = '';
      } else {
        addLog(false, data.error, value);
        showToast(data.error, true);
      }
    } catch (e) {
      addLog(false, e.message, value); showToast(e.message, true);
    } finally {
      btn.disabled = false; btn.textContent = 'Send Order';
    }
  }

  function addLog(ok, meta, body) {
    const li = document.createElement('li');
    li.className = 'log-item ' + (ok ? 'ok' : 'err');
    li.innerHTML = `<span class="log-meta">${meta}</span><span class="log-body">${esc(body)}</span>`;
    document.getElementById('log').prepend(li);
  }

  function clearLog() { document.getElementById('log').innerHTML = ''; }

  function showToast(msg, error = false) {
    const t = document.getElementById('toast');
    t.textContent = msg; t.className = 'toast show' + (error ? ' error' : '');
    setTimeout(() => t.className = 'toast', 2500);
  }

  function esc(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

  document.addEventListener('keydown', e => { if (e.key === 'Enter' && e.metaKey) sendOrder(); });
</script>
</body>
</html>
""";
    }
}
