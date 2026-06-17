package com.capitec.kafka.inventory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;

public class InventoryProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InventoryProducer.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public InventoryProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,              bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,           StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,         StringSerializer.class);
        p.put(ProducerConfig.ACKS_CONFIG,                           "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,             true);
        p.put(ProducerConfig.RETRIES_CONFIG,                        Integer.MAX_VALUE);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,            120_000);
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        p.put(ProducerConfig.LINGER_MS_CONFIG,                      5);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,               "lz4");
        this.producer = new KafkaProducer<>(p);
    }

    // Schema: key=SKU, value={"sku":"...","productID":"...","name":"...","category":"...","quantity":N,"reorderLevel":N,"unitPrice":N.NN,"action":"SET|ADJUST","updatedAt":"..."}
    public void publish(InventoryItem item, String action) {
        String payload = String.format(
            "{\"sku\":\"%s\",\"productID\":\"%s\",\"name\":\"%s\",\"category\":\"%s\"," +
            "\"quantity\":%d,\"reorderLevel\":%d,\"unitPrice\":%.2f,\"action\":\"%s\",\"updatedAt\":\"%s\"}",
            item.sku, item.productID, esc(item.name), esc(item.category),
            item.quantity, item.reorderLevel, item.unitPrice, action, LocalDateTime.now()
        );
        producer.send(new ProducerRecord<>(topic, item.sku, payload), (meta, ex) -> {
            if (ex != null) log.error("Failed to publish inventory sku={}", item.sku, ex);
            else log.info("Inventory published sku={} action={} qty={} offset={}", item.sku, action, item.quantity, meta.offset());
        });
        producer.flush();
    }

    private String esc(String s) { return s == null ? "" : s.replace("\"", "\\\""); }

    @Override public void close() { producer.close(); }
}
