package com.capitec.kafka.orderservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class OrderRepository implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrderRepository.class);
    private final Connection conn;

    // Primary constructor — owns the connection and the DB file
    public OrderRepository(String dbPath) throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        createTable();
        conn.createStatement().execute("PRAGMA journal_mode=WAL");
    }

    // Secondary constructor — shares an existing connection (used when CustomerRepository is on the same DB)
    public OrderRepository(Connection sharedConn) throws SQLException {
        conn = sharedConn;
        conn.createStatement().execute("PRAGMA busy_timeout=5000");
        createTable();
    }

    public Connection getConnection() { return conn; }

    private void createTable() throws SQLException {
        conn.createStatement().execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id             TEXT PRIMARY KEY,
                customer_id          TEXT,
                product              TEXT,
                amount               REAL,
                status               TEXT,
                cancellation_reason  TEXT,
                received_at          TEXT DEFAULT (datetime('now')),
                updated_at           TEXT DEFAULT (datetime('now'))
            )
        """);
        // Safe migration for existing DBs
        try { conn.createStatement().execute("ALTER TABLE orders ADD COLUMN cancellation_reason TEXT"); }
        catch (SQLException ignored) {}
    }

    // Idempotent upsert — re-processing the same orderID is safe
    public synchronized void upsert(Order order) throws SQLException {
        String sql = """
            INSERT INTO orders (order_id, customer_id, product, amount, status, received_at, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(order_id) DO UPDATE SET
                status     = excluded.status,
                updated_at = datetime('now')
        """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, order.orderID);
            ps.setString(2, order.customerID);
            ps.setString(3, order.product);
            ps.setDouble(4, order.amount);
            ps.setString(5, order.status);
            ps.executeUpdate();
        }
    }

    public synchronized void updateCancelled(String orderID, String reason) throws SQLException {
        String sql = "UPDATE orders SET status = 'CANCELLED', cancellation_reason = ?, updated_at = datetime('now') WHERE order_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, reason);
            ps.setString(2, orderID);
            ps.executeUpdate();
        }
    }

    public synchronized void updateStatus(String orderID, String status) throws SQLException {
        String sql = "UPDATE orders SET status = ?, updated_at = datetime('now') WHERE order_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setString(2, orderID);
            ps.executeUpdate();
        }
    }

    // Overload without customerID for backward compatibility
    public List<Order> findAll(String search, String statusFilter, int limit, int offset) throws SQLException {
        return findAll(search, statusFilter, null, limit, offset);
    }

    public List<Order> findAll(String search, String statusFilter, String customerID, int limit, int offset) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "SELECT order_id, customer_id, product, amount, status, cancellation_reason, received_at, updated_at FROM orders WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (search != null && !search.isBlank()) {
            sql.append(" AND (order_id LIKE ? OR customer_id LIKE ? OR product LIKE ?)");
            String like = "%" + search + "%";
            params.add(like); params.add(like); params.add(like);
        }
        if (statusFilter != null && !statusFilter.isBlank() && !"ALL".equals(statusFilter)) {
            sql.append(" AND status = ?");
            params.add(statusFilter);
        }
        if (customerID != null && !customerID.isBlank()) {
            sql.append(" AND customer_id = ?");
            params.add(customerID);
        }
        sql.append(" ORDER BY updated_at DESC LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            ResultSet rs = ps.executeQuery();
            List<Order> orders = new ArrayList<>();
            while (rs.next()) {
                Order o = new Order();
                o.orderID    = rs.getString("order_id");
                o.customerID = rs.getString("customer_id");
                o.product    = rs.getString("product");
                o.amount     = rs.getDouble("amount");
                o.status             = rs.getString("status");
                o.cancellationReason = rs.getString("cancellation_reason");
                o.receivedAt         = rs.getString("received_at");
                o.updatedAt  = rs.getString("updated_at");
                orders.add(o);
            }
            return orders;
        }
    }

    public int count(String search, String statusFilter) throws SQLException {
        return count(search, statusFilter, null);
    }

    public int count(String search, String statusFilter, String customerID) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM orders WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (search != null && !search.isBlank()) {
            sql.append(" AND (order_id LIKE ? OR customer_id LIKE ? OR product LIKE ?)");
            String like = "%" + search + "%";
            params.add(like); params.add(like); params.add(like);
        }
        if (statusFilter != null && !statusFilter.isBlank() && !"ALL".equals(statusFilter)) {
            sql.append(" AND status = ?");
            params.add(statusFilter);
        }
        if (customerID != null && !customerID.isBlank()) {
            sql.append(" AND customer_id = ?");
            params.add(customerID);
        }

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public long totalCount() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM orders");
        return rs.next() ? rs.getLong(1) : 0;
    }

    public long countByStatus(String status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM orders WHERE status = ?")) {
            ps.setString(1, status);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }
}
