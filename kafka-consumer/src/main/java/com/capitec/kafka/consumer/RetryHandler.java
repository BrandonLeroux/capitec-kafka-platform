package com.capitec.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryHandler {

    private static final Logger log = LoggerFactory.getLogger(RetryHandler.class);

    private final int maxAttempts;
    private final long retryBackoffMs;
    private final MessageProcessor processor;
    private final DeadLetterProducer dlt;

    public RetryHandler(int maxAttempts, long retryBackoffMs,
                        MessageProcessor processor, DeadLetterProducer dlt) {
        this.maxAttempts  = maxAttempts;
        this.retryBackoffMs = retryBackoffMs;
        this.processor    = processor;
        this.dlt          = dlt;
    }

    /**
     * Returns true if the message was processed successfully and the offset should be committed.
     * Returns false only when we want to pause and retry the entire batch (should not normally happen).
     *
     * Faulty messages (IllegalArgumentException) skip retries and go straight to DLT.
     * Transient failures retry up to maxAttempts, then go to DLT.
     * Either way the method returns true — the offset is always committed so we move forward.
     */
    public boolean handle(ConsumerRecord<String, String> record) {
        String key   = record.key();
        String value = record.value();

        // Faulty message type — no point retrying, send straight to DLT
        if (isFaultyMessage(value)) {
            String reason = "Invalid message format";
            log.warn("Faulty message skipped key={} reason={}", key, reason);
            dlt.send(key, value, reason);
            return true;
        }

        Exception lastException = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                processor.process(key, value);
                return true; // success — commit this offset
            } catch (IllegalArgumentException e) {
                // Processor itself flagged this as unprocessable mid-run
                log.warn("Unprocessable message key={} attempt={} reason={}", key, attempt, e.getMessage());
                dlt.send(key, value, e.getMessage());
                return true;
            } catch (Exception e) {
                lastException = e;
                log.warn("Processing failed key={} attempt={}/{} error={}", key, attempt, maxAttempts, e.getMessage());
                if (attempt < maxAttempts) {
                    sleep(retryBackoffMs * attempt); // linear back-off
                }
            }
        }

        // All retries exhausted — send to DLT and move on
        String reason = "Max retries (" + maxAttempts + ") exceeded: " + lastException.getMessage();
        log.error("Exhausted retries key={}, sending to DLT", key);
        dlt.send(key, value, reason);
        return true;
    }

    private boolean isFaultyMessage(String value) {
        if (value == null || value.isBlank()) return true;
        String t = value.trim();
        return !t.startsWith("{") && !t.startsWith("[");
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
