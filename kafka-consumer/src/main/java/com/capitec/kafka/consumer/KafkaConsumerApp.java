package com.capitec.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerApp.class);

    public static void main(String[] args) throws IOException {
        Properties envProps = loadConfig();
        String bootstrapServers = envProps.getProperty("bootstrap.servers");
        String topic            = envProps.getProperty("topic");
        String groupId          = envProps.getProperty("group.id");
        int    maxRetries       = Integer.parseInt(envProps.getProperty("max.retries", "3"));
        long   retryBackoffMs   = Long.parseLong(envProps.getProperty("retry.backoff.ms", "500"));

        MessageProcessor  processor   = new MessageProcessor();
        DeadLetterProducer dlt        = new DeadLetterProducer(bootstrapServers, topic);
        RetryHandler       retryHandler = new RetryHandler(maxRetries, retryBackoffMs, processor, dlt);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildConsumerProperties(bootstrapServers, groupId));
        consumer.subscribe(Collections.singletonList(topic));

        // Graceful shutdown on SIGTERM / Ctrl-C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, waking consumer...");
            consumer.wakeup();
        }));

        log.info("Consumer started. topic={} group={} maxRetries={}", topic, groupId, maxRetries);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                // Track offsets per partition — only commit after successful processing
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                for (ConsumerRecord<String, String> record : records) {
                    log.debug("Received partition={} offset={} key={}", record.partition(), record.offset(), record.key());

                    boolean done = retryHandler.handle(record);

                    if (done) {
                        // Commit offset for this specific record (offset + 1 = next to read)
                        offsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                        );
                    }
                }

                // Synchronous commit — only fires after all records in the batch are handled
                if (!offsets.isEmpty()) {
                    consumer.commitSync(offsets);
                    log.debug("Committed offsets: {}", offsets);
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer woken up for shutdown.");
        } finally {
            consumer.commitSync(); // flush any uncommitted offsets before exit
            consumer.close();
            dlt.close();
            log.info("Consumer closed.");
        }
    }

    private static Properties buildConsumerProperties(String bootstrapServers, String groupId) {
        Properties props = new Properties();

        // ── Connectivity ──────────────────────────────────────────────────────
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // ── Deserialisation ───────────────────────────────────────────────────
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // ── Offset management ─────────────────────────────────────────────────
        // Disable auto-commit — we commit manually after processing is complete
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Start from earliest if no committed offset exists for this group
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ── Idempotency ───────────────────────────────────────────────────────
        // isolation.read_committed: only read messages from committed transactions.
        // Combined with manual commit this ensures a record is processed at-least-once
        // and the processor's own idempotency check handles the exactly-once guarantee.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // ── Reliability ───────────────────────────────────────────────────────
        // Session timeout — broker removes consumer from group if no heartbeat within this window
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);

        // Max time between poll() calls before the broker assumes the consumer is dead
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);

        // Limit records per poll so a slow batch doesn't exceed max.poll.interval.ms
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        return props;
    }

    private static Properties loadConfig() throws IOException {
        try (InputStream in = KafkaConsumerApp.class.getResourceAsStream("/consumer.properties")) {
            if (in == null) throw new IOException("consumer.properties not found on classpath");
            Properties props = new Properties();
            props.load(in);
            return props;
        }
    }
}
