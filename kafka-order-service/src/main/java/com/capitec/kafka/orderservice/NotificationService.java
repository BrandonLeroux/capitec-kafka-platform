package com.capitec.kafka.orderservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    /**
     * Simulates sending a payment instruction to the customer.
     * Replace the log statements with real SMTP / SMS / push logic.
     */
    public void sendPaymentInstruction(Order order, Customer customer) {
        if (customer == null) {
            log.warn("No customer record found for customerID={} — notification skipped", order.customerID);
            return;
        }

        String message = String.format(
            "Dear %s, your order %s for %s (R %.2f) is ready for payment. " +
            "Please authorise the payment to proceed.",
            customer.fullName(), order.orderID, order.product, order.amount
        );

        // ── Simulated EMAIL ───────────────────────────────────────────────────
        log.info("[EMAIL → {}] {}", customer.email, message);

        // ── Simulated SMS ────────────────────────────────────────────────────
        log.info("[SMS   → {}] Payment required: {} R{} — Order {}",
                customer.cell, order.product, String.format("%.2f", order.amount), order.orderID);
    }
}
