package com.capitec.kafka.portal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class SequenceService {

    private static final Logger log = LoggerFactory.getLogger(SequenceService.class);
    private static final long START = 1_000_000_000L;

    private final Connection conn;

    public SequenceService(String dbPath, String orderServiceUrl) throws Exception {
        conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        conn.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS customer_sequence (last_number BIGINT NOT NULL)"
        );

        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM customer_sequence");
        boolean empty = rs.next() && rs.getInt(1) == 0;

        if (empty) {
            // Seed from the highest customer number already in the order-service DB
            // so restarts never re-issue an existing number
            long highWatermark = fetchHighWatermark(orderServiceUrl);
            long seed = Math.max(highWatermark, START - 1);
            conn.createStatement().execute("INSERT INTO customer_sequence VALUES (" + seed + ")");
            log.info("Sequence initialised at {} (high watermark from order-service={})", seed + 1, highWatermark);
        } else {
            // Table exists — sync up in case new customers were added to order-service
            // while this pod was down (e.g. via the seed script)
            long localLast  = localLast();
            long remoteLast = fetchHighWatermark(orderServiceUrl);
            if (remoteLast > localLast) {
                conn.createStatement().execute("UPDATE customer_sequence SET last_number = " + remoteLast);
                log.info("Sequence fast-forwarded from {} to {} to match order-service", localLast, remoteLast);
            }
        }
    }

    public synchronized long next() throws Exception {
        conn.createStatement().execute("UPDATE customer_sequence SET last_number = last_number + 1");
        ResultSet rs = conn.createStatement().executeQuery("SELECT last_number FROM customer_sequence");
        long num = rs.next() ? rs.getLong(1) : START;
        log.info("Issued customer number {}", num);
        return num;
    }

    public void close() throws Exception { conn.close(); }

    // ── helpers ───────────────────────────────────────────────────────────────

    private long localLast() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("SELECT last_number FROM customer_sequence");
        return rs.next() ? rs.getLong(1) : START - 1;
    }

    private long fetchHighWatermark(String orderServiceUrl) {
        // GET /api/customers?size=1&sort=customerNumber — order-service returns
        // customers sorted by created_at DESC; we scan up to 1 page to find max.
        // Simpler: call a dedicated max endpoint or just scan the first page.
        // We use /api/customer/max-number if available, else fall back to 0.
        try {
            String url = orderServiceUrl + "/api/customer/max-number";
            HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
            c.setConnectTimeout(3000);
            c.setReadTimeout(3000);
            int code = c.getResponseCode();
            if (code == 200) {
                String body = new String(c.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                // Response: {"maxCustomerNumber": 1000000999}
                String val = extractNum(body, "maxCustomerNumber");
                if (val != null) return Long.parseLong(val.trim());
            }
        } catch (Exception e) {
            log.warn("Could not fetch high watermark from order-service: {}", e.getMessage());
        }
        return START - 1;
    }

    private String extractNum(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return null;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        int end = start;
        while (end < json.length() && ",}".indexOf(json.charAt(end)) < 0) end++;
        return json.substring(start, end).trim();
    }
}
