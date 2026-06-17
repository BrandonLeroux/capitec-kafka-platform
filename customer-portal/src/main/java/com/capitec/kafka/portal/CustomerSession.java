package com.capitec.kafka.portal;

public class CustomerSession {
    public final String token;
    public final String customerID;
    public final long   customerNumber;
    public final String firstName;
    public final String cell;
    public final long   createdAt;

    public CustomerSession(String token, String customerID, long customerNumber, String firstName, String cell) {
        this.token          = token;
        this.customerID     = customerID;
        this.customerNumber = customerNumber;
        this.firstName      = firstName;
        this.cell           = cell;
        this.createdAt      = System.currentTimeMillis();
    }

    public boolean isExpired() {
        return System.currentTimeMillis() - createdAt > 3_600_000; // 1 hour
    }
}
