package com.capitec.kafka.portal;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SessionStore {
    private final ConcurrentHashMap<String, CustomerSession> sessions = new ConcurrentHashMap<>();

    public String create(String customerID, long customerNumber, String firstName, String cell) {
        String token = UUID.randomUUID().toString();
        sessions.put(token, new CustomerSession(token, customerID, customerNumber, firstName, cell));
        return token;
    }

    public CustomerSession get(String token) {
        if (token == null) return null;
        CustomerSession s = sessions.get(token);
        if (s == null || s.isExpired()) { sessions.remove(token); return null; }
        return s;
    }

    public void remove(String token) { sessions.remove(token); }
}
