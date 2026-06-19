package com.capitec.kafka.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

public class FulfilmentWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FulfilmentWorker.class);

    private final String orderID;
    private final String customerID;
    private final String product;
    private final double amount;
    private final StatusProducer producer;
    private final String orderTopic;
    private final String orderServiceUrl;

    public FulfilmentWorker(String orderID, String customerID, String product, double amount,
                            StatusProducer producer, String orderTopic, String orderServiceUrl) {
        this.orderID        = orderID;
        this.customerID     = customerID;
        this.product        = product;
        this.amount         = amount;
        this.producer       = producer;
        this.orderTopic     = orderTopic;
        this.orderServiceUrl = orderServiceUrl;
    }

    @Override
    public void run() {
        try {
            log.info("Payment processing started orderID={}", orderID);

            sleep(2_000);
            if (isCancelled()) { log.info("Order cancelled before PAYMENT-PROCESSED — stopping orderID={}", orderID); return; }
            publish("PAYMENT-PROCESSED");

            sleep(3_000);
            if (isCancelled()) { log.info("Order cancelled before PACKED — stopping orderID={}", orderID); return; }
            publish("PACKED");

            sleep(3_000);
            if (isCancelled()) { log.info("Order cancelled before OUT-FOR-DELIVERY — stopping orderID={}", orderID); return; }
            publish("OUT-FOR-DELIVERY");

            sleep(5_000);
            if (isCancelled()) { log.info("Order cancelled before DELIVERED — stopping orderID={}", orderID); return; }
            publish("DELIVERED");

            log.info("Order fulfilled orderID={}", orderID);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("FulfilmentWorker interrupted orderID={}", orderID);
        }
    }

    private boolean isCancelled() {
        try {
            String url = orderServiceUrl + "/api/orders?search=" + orderID + "&size=1";
            HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
            c.setConnectTimeout(2000); c.setReadTimeout(2000);
            if (c.getResponseCode() != 200) return false;
            String body = new String(c.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            return body.contains("\"CANCELLED\"");
        } catch (Exception e) {
            log.warn("Could not check cancellation for orderID={} — continuing", orderID);
            return false;
        }
    }

    private void publish(String status) {
        String payload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"product\":\"%s\"," +
            "\"amount\":%.2f,\"status\":\"%s\",\"updatedAt\":\"%s\"}",
            orderID, customerID, product, amount, status, LocalDateTime.now()
        );
        producer.publish(orderTopic, orderID, payload);
        log.info("Status published orderID={} status={}", orderID, status);
    }

    private void sleep(long ms) throws InterruptedException { Thread.sleep(ms); }
}
