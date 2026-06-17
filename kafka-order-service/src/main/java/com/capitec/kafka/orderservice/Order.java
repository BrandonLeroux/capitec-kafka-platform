package com.capitec.kafka.orderservice;

public class Order {
    public String orderID;
    public String customerID;
    public String product;
    public double amount;
    public String status;
    public String cancellationReason;
    public String receivedAt;
    public String updatedAt;

    public Order() {}

    public Order(String orderID, String customerID, String product, double amount, String status) {
        this.orderID    = orderID;
        this.customerID = customerID;
        this.product    = product;
        this.amount     = amount;
        this.status     = status;
    }
}
