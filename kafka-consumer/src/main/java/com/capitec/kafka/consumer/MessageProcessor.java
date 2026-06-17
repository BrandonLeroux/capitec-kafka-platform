package com.capitec.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulates business processing. Replace the body of process() with real logic.
 * Throws IllegalArgumentException for structurally invalid messages (→ skip to DLT immediately).
 * Throws RuntimeException for transient failures (→ retry up to max attempts).
 */
public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    public void process(String key, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Message value is null or blank");
        }

        // Faulty message type check — must be valid JSON object
        String trimmed = value.trim();
        if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) {
            throw new IllegalArgumentException("Message is not valid JSON: " + trimmed);
        }

        // ── Real processing logic goes here ──────────────────────────────────
        log.info("Processing key={} value={}", key, value);
        // ─────────────────────────────────────────────────────────────────────
    }
}
