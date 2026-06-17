package com.capitec.kafka.orderservice;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CustomerRepository {

    private final Connection conn;

    public CustomerRepository(Connection conn) throws SQLException {
        this.conn = conn;
        createTable();
        migrate();
    }

    private void createTable() throws SQLException {
        conn.createStatement().execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id      TEXT PRIMARY KEY,
                customer_number  BIGINT,
                first_name       TEXT,
                last_name        TEXT,
                id_number        TEXT,
                email            TEXT,
                cell             TEXT UNIQUE,
                password_hash    TEXT,
                created_at       TEXT DEFAULT (datetime('now'))
            )
        """);
    }

    private void migrate() {
        // Safe migrations for existing DBs that predate these columns
        for (String col : new String[]{
            "ALTER TABLE customers ADD COLUMN customer_number BIGINT",
            "ALTER TABLE customers ADD COLUMN id_number TEXT",
            "ALTER TABLE customers ADD COLUMN password_hash TEXT"
        }) {
            try { conn.createStatement().execute(col); } catch (SQLException ignored) {}
        }
    }

    public synchronized void upsert(Customer c) throws SQLException {
        String sql = """
            INSERT INTO customers
              (customer_id, customer_number, first_name, last_name, id_number, email, cell, password_hash, created_at)
            VALUES (?,?,?,?,?,?,?,?, datetime('now'))
            ON CONFLICT(customer_id) DO UPDATE SET
                customer_number = CASE WHEN excluded.customer_number > 0 THEN excluded.customer_number ELSE customer_number END,
                first_name      = excluded.first_name,
                last_name       = excluded.last_name,
                id_number       = COALESCE(excluded.id_number, id_number),
                email           = excluded.email,
                cell            = excluded.cell,
                password_hash   = COALESCE(excluded.password_hash, password_hash)
        """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, c.customerID);
            ps.setLong(2, c.customerNumber);
            ps.setString(3, c.firstName);
            ps.setString(4, c.lastName);
            ps.setString(5, c.idNumber);
            ps.setString(6, c.email);
            ps.setString(7, c.cell);
            ps.setString(8, c.passwordHash);
            ps.executeUpdate();
        }
    }

    public Customer findById(String customerID) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM customers WHERE customer_id = ?")) {
            ps.setString(1, customerID);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? map(rs) : null;
        }
    }

    public Customer findByCell(String cell) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM customers WHERE cell = ?")) {
            ps.setString(1, cell);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? map(rs) : null;
        }
    }

    public List<Customer> findAll(String search, int limit, int offset) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT * FROM customers WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (customer_id LIKE ? OR first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR cell LIKE ?)");
            String like = "%" + search + "%";
            for (int i = 0; i < 5; i++) params.add(like);
        }
        sql.append(" ORDER BY created_at DESC LIMIT ? OFFSET ?");
        params.add(limit); params.add(offset);
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            ResultSet rs = ps.executeQuery();
            List<Customer> list = new ArrayList<>();
            while (rs.next()) list.add(map(rs));
            return list;
        }
    }

    public long maxCustomerNumber() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT MAX(customer_number) FROM customers");
        return rs.next() ? rs.getLong(1) : 999_999_999L;
    }

    public int count(String search) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM customers WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (search != null && !search.isBlank()) {
            sql.append(" AND (customer_id LIKE ? OR first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR cell LIKE ?)");
            String like = "%" + search + "%";
            for (int i = 0; i < 5; i++) params.add(like);
        }
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private Customer map(ResultSet rs) throws SQLException {
        Customer c = new Customer();
        c.customerID     = rs.getString("customer_id");
        c.customerNumber = rs.getLong("customer_number");
        c.firstName      = rs.getString("first_name");
        c.lastName       = rs.getString("last_name");
        c.idNumber       = rs.getString("id_number");
        c.email          = rs.getString("email");
        c.cell           = rs.getString("cell");
        c.passwordHash   = rs.getString("password_hash");
        c.createdAt      = rs.getString("created_at");
        return c;
    }
}
