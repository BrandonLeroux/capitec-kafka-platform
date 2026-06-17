package com.capitec.kafka.payment;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PaymentProcessorApp {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessorApp.class);

    public static void main(String[] args) throws IOException {
        Properties env = loadConfig();
        String bootstrapServers = env.getProperty("bootstrap.servers");
        String sourceTopic      = env.getProperty("source.topic",  "payment-init");
        String groupId          = env.getProperty("group.id",      "payment-processor-group");
        String orderTopic       = env.getProperty("order.topic",   "order-created");

        StatusProducer producer = new StatusProducer(bootstrapServers);
        ExecutorService pool    = Executors.newFixedThreadPool(10);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       false);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,          "read_committed");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         10);
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       30_000);
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,     300_000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(sourceTopic));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received");
            consumer.wakeup();
        }));

        log.info("Payment processor started. consuming={} publishing-to={}", sourceTopic, orderTopic);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    String json       = record.value();
                    String orderID    = extract(json, "orderID");
                    String customerID = extract(json, "customerID");
                    String product    = extract(json, "product");
                    String amountStr  = extractNum(json, "amount");
                    double amount     = amountStr != null ? Double.parseDouble(amountStr.replace(",", ".")) : 0.0;

                    if (orderID == null) {
                        log.warn("Could not extract orderID from record key={}", record.key());
                    } else {
                        log.info("Payment triggered orderID={}", orderID);
                        pool.submit(new FulfilmentWorker(
                            orderID,
                            customerID != null ? customerID : "",
                            product    != null ? product    : "",
                            amount,
                            producer, orderTopic
                        ));
                    }
                    offsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                    );
                }
                consumer.commitSync(offsets);
            }
        } catch (WakeupException e) {
            log.info("Consumer woken up for shutdown.");
        } finally {
            consumer.commitSync();
            consumer.close();
            producer.close();
            pool.shutdown();
            log.info("Payment processor shut down.");
        }
    }

    private static String extract(String json, String field) {
        if (json == null) return null;
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return null;
        int start = json.indexOf('"', colon + 1);
        if (start < 0) return null;
        int end = json.indexOf('"', start + 1);
        if (end < 0) return null;
        return json.substring(start + 1, end);
    }

    private static String extractNum(String json, String field) {
        if (json == null) return null;
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return null;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        int end = start;
        while (end < json.length() && ",}".indexOf(json.charAt(end)) < 0) end++;
        return json.substring(start, end).trim();
    }

    private static Properties loadConfig() throws IOException {
        try (InputStream in = PaymentProcessorApp.class.getResourceAsStream("/processor.properties")) {
            if (in == null) throw new IOException("processor.properties not found");
            Properties p = new Properties();
            p.load(in);
            return p;
        }
    }
}
