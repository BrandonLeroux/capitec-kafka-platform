package com.capitec.kafka.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class FulfilmentWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FulfilmentWorker.class);

    private final String orderID;
    private final String customerID;
    private final String product;
    private final double amount;
    private final StatusProducer producer;
    private final String orderTopic;

    public FulfilmentWorker(String orderID, String customerID, String product, double amount,
                            StatusProducer producer, String orderTopic) {
        this.orderID    = orderID;
        this.customerID = customerID;
        this.product    = product;
        this.amount     = amount;
        this.producer   = producer;
        this.orderTopic = orderTopic;
    }

    @Override
    public void run() {
        try {
            log.info("Payment processing started orderID={}", orderID);

            sleep(2_000);
            publish("PAYMENT-PROCESSED");

            sleep(3_000);
            publish("PACKED");

            sleep(3_000);
            publish("OUT-FOR-DELIVERY");

            sleep(5_000);
            publish("DELIVERED");

            log.info("Order fulfilled orderID={}", orderID);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("FulfilmentWorker interrupted orderID={}", orderID);
        }
    }

    private void publish(String status) {
        // All status updates go back onto order-created so order-service
        // has a single source of truth for all order lifecycle events.
        String payload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"product\":\"%s\"," +
            "\"amount\":%.2f,\"status\":\"%s\",\"updatedAt\":\"%s\"}",
            orderID, customerID, product, amount, status, LocalDateTime.now()
        );
        producer.publish(orderTopic, orderID, payload);
        log.info("Status published orderID={} status={} topic={}", orderID, status, orderTopic);
    }

    private void sleep(long ms) throws InterruptedException { Thread.sleep(ms); }
}
