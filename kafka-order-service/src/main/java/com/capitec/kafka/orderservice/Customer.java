package com.capitec.kafka.orderservice;

public class Customer {
    public String customerID;
    public String firstName;
    public String lastName;
    public String idNumber;
    public String email;
    public String cell;
    public String passwordHash;
    public long   customerNumber;
    public String createdAt;

    public Customer() {}

    public Customer(String customerID, String firstName, String lastName,
                    String idNumber, String email, String cell,
                    String passwordHash, long customerNumber) {
        this.customerID     = customerID;
        this.firstName      = firstName;
        this.lastName       = lastName;
        this.idNumber       = idNumber;
        this.email          = email;
        this.cell           = cell;
        this.passwordHash   = passwordHash;
        this.customerNumber = customerNumber;
    }

    public String fullName() { return firstName + " " + lastName; }
}
