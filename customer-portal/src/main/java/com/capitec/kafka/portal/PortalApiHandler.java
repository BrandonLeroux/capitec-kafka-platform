package com.capitec.kafka.portal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

public class PortalApiHandler {

    private static final Logger log = LoggerFactory.getLogger(PortalApiHandler.class);

    public static void handle(String method, String path, String query, String body,
                              String token, OutputStream out) throws IOException {
        if ("GET".equals(method) && "/".equals(path)) {
            PortalApp.respond(out, 200, "text/html; charset=UTF-8", PortalHtmlServer.html());
        } else if ("POST".equals(method) && "/api/login".equals(path)) {
            handleLogin(body, out);
        } else if ("POST".equals(method) && "/api/register".equals(path)) {
            handleRegister(body, out);
        } else if ("POST".equals(method) && "/api/logout".equals(path)) {
            PortalApp.sessionStore.remove(token);
            PortalApp.respond(out, 200, "application/json", "{\"ok\":true}",
                "session=; Max-Age=0; Path=/");
        } else if ("GET".equals(method) && "/api/me".equals(path)) {
            handleMe(token, out);
        } else if ("GET".equals(method) && "/api/my-orders".equals(path)) {
            handleMyOrders(token, query, out);
        } else if ("POST".equals(method) && "/api/order".equals(path)) {
            handleOrder(body, token, out);
        } else if ("PUT".equals(method) && "/api/profile".equals(path)) {
            handleUpdateProfile(body, token, out);
        } else if ("POST".equals(method) && "/api/cancel".equals(path)) {
            handleCancel(body, token, out);
        } else if ("GET".equals(method) && "/api/stock".equals(path)) {
            handleStock(out);
        } else {
            PortalApp.respond(out, 404, "application/json", "{\"error\":\"not found\"}");
        }
    }

    // ── Login ─────────────────────────────────────────────────────────────────
    private static void handleLogin(String body, OutputStream out) throws IOException {
        String cell     = PortalApp.getJson(body, "cell");
        String password = PortalApp.getJson(body, "password");
        if (cell == null || password == null) {
            PortalApp.respond(out, 400, "application/json", "{\"error\":\"cell and password required\"}"); return;
        }

        String customerJson = httpGet(PortalApp.orderServiceUrl + "/api/customer/by-identifier?q=" + cell);
        if (customerJson == null) {
            PortalApp.respond(out, 401, "application/json", "{\"error\":\"Customer not found\"}"); return;
        }

        // Field may arrive as passwordHash or password_hash depending on DB column mapping
        String storedHash = PortalApp.getJson(customerJson, "passwordHash");
        if (storedHash == null) storedHash = PortalApp.getJson(customerJson, "password_hash");
        if (storedHash == null || storedHash.isBlank()) {
            PortalApp.respond(out, 401, "application/json", "{\"error\":\"No password set for this account\"}"); return;
        }
        if (!storedHash.equals(PortalApp.sha256(password))) {
            PortalApp.respond(out, 401, "application/json", "{\"error\":\"Invalid password\"}"); return;
        }

        String customerID = PortalApp.getJson(customerJson, "customerId");
        if (customerID == null) customerID = PortalApp.getJson(customerJson, "customerID");
        String numStr     = PortalApp.getJsonNum(customerJson, "customerNumber");
        long   custNum    = numStr != null ? Long.parseLong(numStr.trim()) : 0L;
        String firstName  = PortalApp.getJson(customerJson, "firstName");

        String token = PortalApp.sessionStore.create(customerID, custNum, firstName, cell);
        log.info("Login success customerID={}", customerID);

        String responseJson = String.format(
            "{\"customerID\":\"%s\",\"customerNumber\":%d,\"firstName\":\"%s\"}",
            PortalApp.esc(customerID), custNum, PortalApp.esc(firstName));
        PortalApp.respond(out, 200, "application/json", responseJson,
            "session=" + token + "; Path=/; HttpOnly; Max-Age=3600");
    }

    // ── Register ──────────────────────────────────────────────────────────────
    private static void handleRegister(String body, OutputStream out) throws IOException {
        String firstName = PortalApp.getJson(body, "firstName");
        String lastName  = PortalApp.getJson(body, "lastName");
        String idNumber  = PortalApp.getJson(body, "idNumber");
        String email     = PortalApp.getJson(body, "email");
        String cell      = PortalApp.getJson(body, "cell");
        String password  = PortalApp.getJson(body, "password");

        // F2: Server-side blank validation
        if (firstName == null || firstName.isBlank()) {
            PortalApp.respond(out, 400, "application/json", "{\"error\":\"First name is required.\"}"); return;
        }
        if (lastName == null || lastName.isBlank()) {
            PortalApp.respond(out, 400, "application/json", "{\"error\":\"Last name is required.\"}"); return;
        }
        if (cell == null || cell.isBlank()) {
            PortalApp.respond(out, 400, "application/json", "{\"error\":\"Cell number is required.\"}"); return;
        }
        if (password == null || password.isBlank()) {
            PortalApp.respond(out, 400, "application/json", "{\"error\":\"Password is required.\"}"); return;
        }

        // F1: Check for duplicate cell before consuming a sequence number
        String existingJson = httpGet(PortalApp.orderServiceUrl + "/api/customer/by-identifier?q=" + cell);
        if (existingJson != null && (existingJson.contains("\"customerId\"") || existingJson.contains("\"customerID\""))) {
            PortalApp.respond(out, 400, "application/json", "{\"error\":\"A customer with this cell number already exists.\"}"); return;
        }

        long custNum;
        try { custNum = PortalApp.sequenceService.next(); }
        catch (Exception e) {
            PortalApp.respond(out, 500, "application/json", "{\"error\":\"Sequence error\"}"); return;
        }

        String customerID   = String.valueOf(custNum);
        String passwordHash = PortalApp.sha256(password);

        String payload = String.format(
            "{\"customerID\":\"%s\",\"customerNumber\":%d,\"firstName\":\"%s\",\"lastName\":\"%s\"," +
            "\"idNumber\":\"%s\",\"email\":\"%s\",\"cell\":\"%s\",\"passwordHash\":\"%s\"}",
            customerID, custNum,
            PortalApp.esc(firstName), PortalApp.esc(lastName),
            PortalApp.esc(idNumber != null ? idNumber : ""),
            PortalApp.esc(email    != null ? email    : ""),
            PortalApp.esc(cell), passwordHash
        );

        PortalApp.kafkaProducer.send(PortalApp.customerTopic, customerID, payload);
        log.info("Registered customerID={} number={}", customerID, custNum);

        PortalApp.respond(out, 200, "application/json",
            String.format("{\"customerID\":\"%s\",\"customerNumber\":%d}", customerID, custNum));
    }

    // ── Me ────────────────────────────────────────────────────────────────────
    private static void handleMe(String token, OutputStream out) throws IOException {
        CustomerSession s = PortalApp.sessionStore.get(token);
        if (s == null) { PortalApp.respond(out, 401, "application/json", "{\"error\":\"Not logged in\"}"); return; }
        String customerJson = httpGet(PortalApp.orderServiceUrl + "/api/customer/" + s.customerID);
        if (customerJson == null) { PortalApp.respond(out, 404, "application/json", "{\"error\":\"Not found\"}"); return; }
        PortalApp.respond(out, 200, "application/json", customerJson);
    }

    // ── My orders ─────────────────────────────────────────────────────────────
    private static void handleMyOrders(String token, String query, OutputStream out) throws IOException {
        CustomerSession s = PortalApp.sessionStore.get(token);
        if (s == null) { PortalApp.respond(out, 401, "application/json", "{\"error\":\"Not logged in\"}"); return; }
        String ordersJson = httpGet(PortalApp.orderServiceUrl + "/api/orders?customerID=" + s.customerID + "&size=50");
        PortalApp.respond(out, 200, "application/json", ordersJson != null ? ordersJson : "{\"orders\":[]}");
    }

    // ── Submit order ──────────────────────────────────────────────────────────
    private static void handleOrder(String body, String token, OutputStream out) throws IOException {
        CustomerSession s = PortalApp.sessionStore.get(token);
        if (s == null) { PortalApp.respond(out, 401, "application/json", "{\"error\":\"Not logged in\"}"); return; }

        String product = PortalApp.getJson(body, "product");
        String amountS = PortalApp.getJsonNum(body, "amount");
        String qtyS    = PortalApp.getJsonNum(body, "qty");
        if (product == null || amountS == null || amountS.isBlank()) {
            PortalApp.respond(out, 400, "application/json", "{\"error\":\"product and amount required\"}"); return;
        }

        double amount = Double.parseDouble(amountS.replace(",", "."));
        int    qty    = (qtyS != null && !qtyS.isBlank()) ? Integer.parseInt(qtyS.trim()) : 1;

        // Server-side stock check — prevent ordering more than available
        String stockJson = httpGet(PortalApp.orderServiceUrl + "/api/inventory/stock");
        if (stockJson != null) {
            String qtyAvailStr = PortalApp.getJsonNum(
                stockJson.contains("\"" + product + "\"") ? stockJson.substring(stockJson.indexOf("\"" + product + "\"")) : "",
                "qty");
            if (qtyAvailStr != null && !qtyAvailStr.isBlank()) {
                int available = Integer.parseInt(qtyAvailStr.trim());
                if (qty > available) {
                    PortalApp.respond(out, 400, "application/json",
                        String.format("{\"error\":\"Only %d unit%s in stock\"}", available, available == 1 ? "" : "s"));
                    return;
                }
                if (available == 0) {
                    PortalApp.respond(out, 400, "application/json", "{\"error\":\"Item is out of stock\"}");
                    return;
                }
            }
        }

        String orderID = "ORD-" + s.customerNumber + "-" + System.currentTimeMillis();

        String payload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"product\":\"%s\"," +
            "\"amount\":%.2f,\"qty\":%d,\"status\":\"CONFIRMED\",\"createdAt\":\"%s\"}",
            orderID, s.customerID, product, amount, qty, LocalDateTime.now()
        );

        PortalApp.kafkaProducer.send(PortalApp.orderTopic, orderID, payload);

        // Publish inventory ADJUST event (deduct ordered qty)
        // Schema: {"sku":"<sku>","productID":"<product>","quantity":<-qty>,"action":"ADJUST"}
        // The inventory-service uses the SKU as key, so we look it up via the stock endpoint.
        // For simplicity we publish with productID and let inventory-service map it.
        String invPayload = String.format(
            "{\"productID\":\"%s\",\"quantity\":%d,\"action\":\"ADJUST\",\"orderID\":\"%s\"}",
            PortalApp.esc(product), -qty, PortalApp.esc(orderID));
        PortalApp.kafkaProducer.send("inventory", product, invPayload);

        log.info("Order submitted orderID={} customerID={} product={} qty={}", orderID, s.customerID, product, qty);
        PortalApp.respond(out, 200, "application/json",
            String.format("{\"orderID\":\"%s\"}", orderID));
    }

    // ── Cancel order ──────────────────────────────────────────────────────────
    private static void handleCancel(String body, String token, OutputStream out) throws IOException {
        CustomerSession s = PortalApp.sessionStore.get(token);
        if (s == null) { PortalApp.respond(out, 401, "application/json", "{\"error\":\"Not logged in\"}"); return; }

        String orderID = PortalApp.getJson(body, "orderID");
        String reason  = PortalApp.getJson(body, "reason");
        if (orderID == null) { PortalApp.respond(out, 400, "application/json", "{\"error\":\"orderID required\"}"); return; }
        if (reason == null || reason.isBlank()) reason = "Customer requested cancellation";

        // Publish to order-cancelled topic
        // Schema: {"orderID":"...","customerID":"...","reason":"...","cancelledAt":"..."}
        String payload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"reason\":\"%s\",\"cancelledAt\":\"%s\"}",
            PortalApp.esc(orderID), PortalApp.esc(s.customerID),
            PortalApp.esc(reason), java.time.LocalDateTime.now()
        );

        PortalApp.kafkaProducer.send("order-cancelled", orderID, payload);

        // Look up order details to restore inventory stock
        String orderJson = httpGet(PortalApp.orderServiceUrl + "/api/orders?customerID=" + s.customerID + "&size=100");
        if (orderJson != null) {
            // Find the specific order in the results
            String product = null; int qty = 1;
            int idx = orderJson.indexOf("\"" + orderID + "\"");
            if (idx >= 0) {
                // Extract product and qty from the order record context
                String segment = orderJson.substring(Math.max(0, idx - 20), Math.min(orderJson.length(), idx + 300));
                product = PortalApp.getJson(segment, "product");
                String qtyStr = PortalApp.getJsonNum(segment, "qty");
                if (qtyStr != null && !qtyStr.isBlank()) qty = Integer.parseInt(qtyStr.trim());
            }
            if (product != null) {
                String invPayload = String.format(
                    "{\"productID\":\"%s\",\"quantity\":%d,\"action\":\"ADJUST\",\"orderID\":\"%s\",\"reason\":\"cancellation\"}",
                    PortalApp.esc(product), qty, PortalApp.esc(orderID));
                PortalApp.kafkaProducer.send("inventory", product, invPayload);
                log.info("Inventory restore published product={} qty=+{} for cancelled orderID={}", product, qty, orderID);
            }
        }

        log.info("Cancellation published orderID={} customerID={}", orderID, s.customerID);
        PortalApp.respond(out, 200, "application/json", "{\"ok\":true,\"orderID\":\"" + PortalApp.esc(orderID) + "\"}");
    }

    // ── Stock levels proxy ─────────────────────────────────────────────────────
    private static void handleStock(OutputStream out) throws IOException {
        // Proxy to order-service which proxies to inventory-service
        String stock = httpGet(PortalApp.orderServiceUrl + "/api/inventory/stock");
        PortalApp.respond(out, 200, "application/json", stock != null ? stock : "{}");
    }

    // ── Update profile ────────────────────────────────────────────────────────
    private static void handleUpdateProfile(String body, String token, OutputStream out) throws IOException {
        CustomerSession s = PortalApp.sessionStore.get(token);
        if (s == null) { PortalApp.respond(out, 401, "application/json", "{\"error\":\"Not logged in\"}"); return; }

        // POST to order-service register endpoint to upsert
        String result = httpPost(PortalApp.orderServiceUrl + "/api/customer/register", body);
        PortalApp.respond(out, 200, "application/json", result != null ? result : "{\"ok\":true}");
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────
    static String httpGet(String urlStr) {
        try {
            HttpURLConnection c = (HttpURLConnection) new URL(urlStr).openConnection();
            c.setConnectTimeout(3000); c.setReadTimeout(3000);
            int code = c.getResponseCode();
            if (code == 404) return null;
            try (InputStream in = c.getInputStream()) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.warn("HTTP GET failed url={} error={}", urlStr, e.getMessage());
            return null;
        }
    }

    static String httpPost(String urlStr, String jsonBody) {
        try {
            HttpURLConnection c = (HttpURLConnection) new URL(urlStr).openConnection();
            c.setRequestMethod("POST");
            c.setDoOutput(true);
            c.setRequestProperty("Content-Type", "application/json");
            c.setConnectTimeout(3000); c.setReadTimeout(3000);
            try (OutputStream os = c.getOutputStream()) {
                os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
            }
            try (InputStream in = c.getInputStream()) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.warn("HTTP POST failed url={} error={}", urlStr, e.getMessage());
            return null;
        }
    }
}
