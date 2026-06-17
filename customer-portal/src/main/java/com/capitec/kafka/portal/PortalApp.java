package com.capitec.kafka.portal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PortalApp {

    private static final Logger log = LoggerFactory.getLogger(PortalApp.class);

    static String orderServiceUrl;
    static String orderTopic;
    static String customerTopic;
    static PortalKafkaProducer kafkaProducer;
    static SequenceService sequenceService;
    static SessionStore sessionStore = new SessionStore();

    public static void main(String[] args) throws Exception {
        Properties env = loadConfig();
        int port         = Integer.parseInt(env.getProperty("port", "8082"));
        orderServiceUrl  = env.getProperty("order.service.url");
        orderTopic       = env.getProperty("order.topic");
        customerTopic    = env.getProperty("customer.topic");
        String dbPath    = env.getProperty("db.path", "/data/portal.db");

        kafkaProducer  = new PortalKafkaProducer(env.getProperty("bootstrap.servers"));
        sequenceService = new SequenceService(dbPath, orderServiceUrl);

        ExecutorService pool = Executors.newFixedThreadPool(8);
        try (ServerSocket server = new ServerSocket(port)) {
            log.info("Customer portal ready at http://localhost:{}", port);
            while (true) {
                Socket socket = server.accept();
                pool.submit(() -> handle(socket));
            }
        }
    }

    static void handle(Socket socket) {
        try (socket;
             BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             OutputStream   out = socket.getOutputStream()) {

            String requestLine = in.readLine();
            if (requestLine == null) return;
            String[] parts = requestLine.split(" ");
            String method  = parts[0];
            String rawPath = parts.length > 1 ? parts[1] : "/";

            int contentLength = 0;
            String cookieHeader = null;
            String line;
            while ((line = in.readLine()) != null && !line.isEmpty()) {
                String lower = line.toLowerCase();
                if (lower.startsWith("content-length:")) contentLength = Integer.parseInt(line.split(":",2)[1].trim());
                if (lower.startsWith("cookie:")) cookieHeader = line.substring(7).trim();
            }
            String body = "";
            if (contentLength > 0) {
                char[] buf = new char[contentLength];
                in.read(buf, 0, contentLength);
                body = new String(buf);
            }

            String token = extractCookie(cookieHeader, "session");
            String path  = rawPath.contains("?") ? rawPath.substring(0, rawPath.indexOf('?')) : rawPath;
            String query = rawPath.contains("?") ? rawPath.substring(rawPath.indexOf('?') + 1) : "";

            PortalApiHandler.handle(method, path, query, body, token, out);

        } catch (Exception e) {
            log.error("Request error", e);
        }
    }

    static String extractCookie(String cookieHeader, String name) {
        if (cookieHeader == null) return null;
        for (String part : cookieHeader.split(";")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length == 2 && kv[0].trim().equals(name)) return kv[1].trim();
        }
        return null;
    }

    static String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    static String param(String query, String name) throws UnsupportedEncodingException {
        if (query == null || query.isBlank()) return null;
        for (String part : query.split("&")) {
            String[] kv = part.split("=", 2);
            if (kv.length == 2 && kv[0].equals(name)) return URLDecoder.decode(kv[1], "UTF-8");
        }
        return null;
    }

    static String getJson(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return null;
        int start = json.indexOf('"', colon + 1);
        if (start < 0) return null;
        int end = start + 1;
        while (end < json.length()) {
            if (json.charAt(end) == '"' && json.charAt(end-1) != '\\') break;
            end++;
        }
        return json.substring(start + 1, end).replace("\\\"", "\"");
    }

    static String getJsonNum(String json, String field) {
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

    static void respond(OutputStream out, int status, String contentType, String body) throws IOException {
        respond(out, status, contentType, body, null);
    }

    static void respond(OutputStream out, int status, String contentType, String body, String setCookie) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        StringBuilder headers = new StringBuilder();
        headers.append("HTTP/1.1 ").append(status).append(" OK\r\n");
        headers.append("Content-Type: ").append(contentType).append("\r\n");
        headers.append("Content-Length: ").append(bytes.length).append("\r\n");
        if (setCookie != null) headers.append("Set-Cookie: ").append(setCookie).append("\r\n");
        headers.append("Connection: close\r\n\r\n");
        out.write(headers.toString().getBytes(StandardCharsets.UTF_8));
        out.write(bytes);
        out.flush();
    }

    static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\","\\\\").replace("\"","\\\"");
    }

    private static Properties loadConfig() throws IOException {
        try (InputStream in = PortalApp.class.getResourceAsStream("/portal.properties")) {
            if (in == null) throw new IOException("portal.properties not found");
            Properties p = new Properties();
            p.load(in);
            return p;
        }
    }
}
