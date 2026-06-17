package com.capitec.kafka.portal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PortalKafkaProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PortalKafkaProducer.class);
    private final KafkaProducer<String, String> producer;

    public PortalKafkaProducer(String bootstrapServers) {
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

    public void send(String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value), (meta, ex) -> {
            if (ex != null) log.error("Failed to send to topic={} key={}", topic, key, ex);
            else log.info("Sent topic={} key={} offset={}", topic, key, meta.offset());
        });
        producer.flush();
    }

    @Override public void close() { producer.close(); }
}
