package com.capitec.kafka.inventory;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InventoryServiceApp {

    private static final Logger log = LoggerFactory.getLogger(InventoryServiceApp.class);

    static InventoryRepository repo;

    public static void main(String[] args) throws Exception {
        Properties env = loadConfig();
        String bootstrapServers = env.getProperty("bootstrap.servers");
        String inventoryTopic   = env.getProperty("inventory.topic",  "inventory");
        String groupId          = env.getProperty("group.id",         "inventory-service-group");
        String dbPath           = env.getProperty("db.path",          "/data/inventory.db");
        int    uiPort           = Integer.parseInt(env.getProperty("ui.port", "8083"));

        repo = new InventoryRepository(dbPath);

        // Start REST server
        startRestServer(uiPort);

        // Consume inventory topic only — all stock adjustments flow through here
        // (orders and cancellations publish ADJUST events directly to this topic)
        KafkaConsumer<String, String> consumer = buildConsumer(bootstrapServers, groupId);
        consumer.subscribe(Collections.singletonList(inventoryTopic));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received");
            consumer.wakeup();
        }));

        log.info("Inventory service started. consuming={}", inventoryTopic);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processInventoryEvent(record);
                    } catch (Exception e) {
                        log.error("Failed to process record topic={} key={}", record.topic(), record.key(), e);
                    }
                    offsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                    );
                }
                consumer.commitSync(offsets);
            }
        } catch (WakeupException e) {
            log.info("Consumer woken up.");
        } finally {
            consumer.commitSync();
            consumer.close();
            repo.close();
            log.info("Inventory service shut down.");
        }
    }

    // ── Inventory event ───────────────────────────────────────────────────────
    // Schema: {"sku":"TYR-175-65-R14","productID":"TYRE_175_65_R14","name":"...","category":"Tyres",
    //          "quantity":50,"reorderLevel":10,"unitPrice":849.00,"action":"SET|ADJUST"}
    private static void processInventoryEvent(ConsumerRecord<String, String> record) throws Exception {
        String json = record.value();
        InventoryItem item = new InventoryItem();
        item.sku          = getString(json, "sku");
        item.productID    = getString(json, "productID");
        item.name         = getString(json, "name");
        item.category     = getString(json, "category");
        item.quantity     = getInt(json, "quantity");
        item.reorderLevel = getInt(json, "reorderLevel");
        item.unitPrice    = getDouble(json, "unitPrice");

        // If sku is missing but productID is present, resolve sku via DB lookup
        if (item.sku == null && item.productID != null) {
            InventoryItem existing = repo.findByProductId(item.productID);
            if (existing != null) item.sku = existing.sku;
        }
        if (item.sku == null) { log.warn("Inventory event: cannot resolve sku, key={}", record.key()); return; }

        String action = getString(json, "action");
        if ("ADJUST".equals(action)) {
            repo.adjustQuantity(item.sku, item.quantity); // quantity = signed delta
            log.info("Inventory adjusted sku={} delta={}", item.sku, item.quantity);
        } else {
            repo.upsert(item);
            log.info("Inventory set sku={} qty={}", item.sku, item.quantity);
        }
    }

    // ── REST server ───────────────────────────────────────────────────────────
    private static void startRestServer(int port) {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Thread t = new Thread(() -> {
            try (ServerSocket server = new ServerSocket(port)) {
                log.info("Inventory REST API ready on port {}", port);
                while (true) {
                    Socket socket = server.accept();
                    pool.submit(() -> handleHttp(socket));
                }
            } catch (Exception e) { log.error("REST server error", e); }
        });
        t.setDaemon(true);
        t.start();
    }

    private static void handleHttp(Socket socket) {
        try (socket;
             BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             OutputStream   out = socket.getOutputStream()) {

            String reqLine = in.readLine();
            if (reqLine == null) return;
            String[] parts = reqLine.split(" ");
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

            if ("GET".equals(method) && "/api/inventory".equals(path)) {
                respond(out, 200, inventoryJson(query));
            } else if ("GET".equals(method) && "/api/inventory/stock".equals(path)) {
                // Lightweight endpoint: returns map of productID → quantity for shop display
                respond(out, 200, stockMapJson());
            } else if ("GET".equals(method) && path.startsWith("/api/inventory/sku/")) {
                String sku = path.substring("/api/inventory/sku/".length());
                InventoryItem item = repo.findBySku(sku);
                respond(out, item != null ? 200 : 404, item != null ? toJson(item) : "{\"error\":\"not found\"}");
            } else if ("POST".equals(method) && "/api/inventory/seed".equals(path)) {
                // Direct DB upsert — used by stock-inventory.sh
                InventoryItem item = new InventoryItem();
                item.sku          = getString(body, "sku");
                item.productID    = getString(body, "productID");
                item.name         = getString(body, "name");
                item.category     = getString(body, "category");
                item.quantity     = getInt(body, "quantity");
                item.reorderLevel = getInt(body, "reorderLevel");
                item.unitPrice    = getDouble(body, "unitPrice");
                if (item.sku == null) { respond(out, 400, "{\"error\":\"sku required\"}"); }
                else { repo.upsert(item); respond(out, 200, "{\"sku\":\"" + esc(item.sku) + "\"}"); }
            } else {
                respond(out, 404, "{\"error\":\"not found\"}");
            }

        } catch (Exception e) { log.error("HTTP handler error", e); }
    }

    private static String inventoryJson(String query) throws Exception {
        String search   = param(query, "search");
        String category = param(query, "category");
        int page     = intParam(query, "page", 1);
        int pageSize = intParam(query, "size", 50);
        int offset   = (page - 1) * pageSize;

        List<InventoryItem> items = repo.findAll(search, category, pageSize, offset);
        int total     = repo.count(search, category);
        long lowStock = repo.countLowStock();

        StringBuilder sb = new StringBuilder();
        sb.append("{\"total\":").append(total)
          .append(",\"lowStock\":").append(lowStock)
          .append(",\"items\":[");
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(toJson(items.get(i)));
        }
        sb.append("]}");
        return sb.toString();
    }

    private static String stockMapJson() throws Exception {
        List<InventoryItem> all = repo.findAll(null, null, 1000, 0);
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < all.size(); i++) {
            if (i > 0) sb.append(",");
            InventoryItem item = all.get(i);
            sb.append("\"").append(esc(item.productID)).append("\":")
              .append("{\"qty\":").append(item.quantity)
              .append(",\"sku\":\"").append(esc(item.sku)).append("\"}");
        }
        sb.append("}");
        return sb.toString();
    }

    private static String toJson(InventoryItem i) {
        return String.format(
            "{\"sku\":\"%s\",\"productID\":\"%s\",\"name\":\"%s\",\"category\":\"%s\"," +
            "\"quantity\":%d,\"reorderLevel\":%d,\"unitPrice\":%.2f,\"updatedAt\":\"%s\"}",
            esc(i.sku), esc(i.productID), esc(i.name), esc(i.category),
            i.quantity, i.reorderLevel, i.unitPrice, esc(i.updatedAt));
    }

    private static void respond(OutputStream out, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        String headers = "HTTP/1.1 " + status + " OK\r\n" +
            "Content-Type: application/json\r\n" +
            "Content-Length: " + bytes.length + "\r\n" +
            "Access-Control-Allow-Origin: *\r\n" +
            "Connection: close\r\n\r\n";
        out.write(headers.getBytes(StandardCharsets.UTF_8));
        out.write(bytes);
        out.flush();
    }

    // ── JSON helpers ──────────────────────────────────────────────────────────
    static String getString(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return null;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        if (start >= json.length() || json.charAt(start) != '"') return null;
        start++;
        int end = start;
        while (end < json.length()) {
            if (json.charAt(end) == '"' && json.charAt(end - 1) != '\\') break;
            end++;
        }
        return json.substring(start, end).replace("\\\"", "\"");
    }

    static int getInt(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return 0;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return 0;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        if (json.charAt(start) == '"') start++;
        int end = start;
        while (end < json.length() && ",}\"".indexOf(json.charAt(end)) < 0) end++;
        try { return Integer.parseInt(json.substring(start, end).trim()); } catch (Exception e) { return 0; }
    }

    static double getDouble(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return 0.0;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return 0.0;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        if (json.charAt(start) == '"') start++;
        int end = start;
        while (end < json.length() && ",}\"".indexOf(json.charAt(end)) < 0) end++;
        try { return Double.parseDouble(json.substring(start, end).trim().replace(',', '.')); } catch (Exception e) { return 0.0; }
    }

    private static String param(String query, String name) throws UnsupportedEncodingException {
        if (query == null || query.isBlank()) return null;
        for (String part : query.split("&")) {
            String[] kv = part.split("=", 2);
            if (kv.length == 2 && kv[0].equals(name)) return URLDecoder.decode(kv[1], "UTF-8");
        }
        return null;
    }

    private static int intParam(String query, String name, int def) throws UnsupportedEncodingException {
        String v = param(query, name);
        try { return v != null ? Integer.parseInt(v) : def; } catch (NumberFormatException e) { return def; }
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static KafkaConsumer<String, String> buildConsumer(String bootstrapServers, String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,          "read_committed");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       30_000);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         50);
        return new KafkaConsumer<>(p);
    }

    private static Properties loadConfig() throws IOException {
        try (InputStream in = InventoryServiceApp.class.getResourceAsStream("/inventory.properties")) {
            if (in == null) throw new IOException("inventory.properties not found");
            Properties p = new Properties(); p.load(in); return p;
        }
    }
}
