package com.capitec.kafka.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class InventoryRepository implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InventoryRepository.class);
    private final Connection conn;

    public InventoryRepository(String dbPath) throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath + "?journal_mode=WAL&busy_timeout=10000");
        createTable();
    }

    private void createTable() throws SQLException {
        conn.createStatement().execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                sku            TEXT PRIMARY KEY,
                product_id     TEXT NOT NULL,
                name           TEXT,
                category       TEXT,
                quantity       INTEGER DEFAULT 0,
                reorder_level  INTEGER DEFAULT 10,
                unit_price     REAL    DEFAULT 0,
                updated_at     TEXT    DEFAULT (datetime('now'))
            )
        """);
    }

    public synchronized void upsert(InventoryItem item) throws SQLException {
        String sql = """
            INSERT INTO inventory (sku, product_id, name, category, quantity, reorder_level, unit_price, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(sku) DO UPDATE SET
                quantity      = excluded.quantity,
                name          = COALESCE(excluded.name, name),
                category      = COALESCE(excluded.category, category),
                reorder_level = COALESCE(excluded.reorder_level, reorder_level),
                unit_price    = COALESCE(excluded.unit_price, unit_price),
                updated_at    = datetime('now')
        """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, item.sku);
            ps.setString(2, item.productID);
            ps.setString(3, item.name);
            ps.setString(4, item.category);
            ps.setInt(5, item.quantity);
            ps.setInt(6, item.reorderLevel);
            ps.setDouble(7, item.unitPrice);
            ps.executeUpdate();
        }
    }

    public synchronized void adjustQuantity(String sku, int delta) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE inventory SET quantity = MAX(0, quantity + ?), updated_at = datetime('now') WHERE sku = ?")) {
            ps.setInt(1, delta);
            ps.setString(2, sku);
            ps.executeUpdate();
        }
    }

    public synchronized InventoryItem findBySku(String sku) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM inventory WHERE sku = ?")) {
            ps.setString(1, sku);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? map(rs) : null;
        }
    }

    public synchronized InventoryItem findByProductId(String productId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM inventory WHERE product_id = ?")) {
            ps.setString(1, productId);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? map(rs) : null;
        }
    }

    public synchronized List<InventoryItem> findAll(String search, String category, int limit, int offset) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT * FROM inventory WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (sku LIKE ? OR name LIKE ? OR product_id LIKE ?)");
            String like = "%" + search + "%";
            params.add(like); params.add(like); params.add(like);
        }
        if (category != null && !category.isBlank() && !"ALL".equals(category)) {
            sql.append(" AND category = ?");
            params.add(category);
        }
        sql.append(" ORDER BY category, name LIMIT ? OFFSET ?");
        params.add(limit); params.add(offset);
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            ResultSet rs = ps.executeQuery();
            List<InventoryItem> list = new ArrayList<>();
            while (rs.next()) list.add(map(rs));
            return list;
        }
    }

    public synchronized int count(String search, String category) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM inventory WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (sku LIKE ? OR name LIKE ? OR product_id LIKE ?)");
            String like = "%" + search + "%";
            params.add(like); params.add(like); params.add(like);
        }
        if (category != null && !category.isBlank() && !"ALL".equals(category)) {
            sql.append(" AND category = ?");
            params.add(category);
        }
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public synchronized long countLowStock() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT COUNT(*) FROM inventory WHERE quantity <= reorder_level");
        return rs.next() ? rs.getLong(1) : 0;
    }

    public synchronized int totalItems() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM inventory");
        return rs.next() ? rs.getInt(1) : 0;
    }

    private InventoryItem map(ResultSet rs) throws SQLException {
        InventoryItem i = new InventoryItem();
        i.sku          = rs.getString("sku");
        i.productID    = rs.getString("product_id");
        i.name         = rs.getString("name");
        i.category     = rs.getString("category");
        i.quantity     = rs.getInt("quantity");
        i.reorderLevel = rs.getInt("reorder_level");
        i.unitPrice    = rs.getDouble("unit_price");
        i.updatedAt    = rs.getString("updated_at");
        return i;
    }

    @Override public void close() throws SQLException { conn.close(); }
}
