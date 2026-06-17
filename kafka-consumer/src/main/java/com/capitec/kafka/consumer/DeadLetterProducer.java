package com.capitec.kafka.consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class DeadLetterProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterProducer.class);

    private final KafkaProducer<String, String> producer;
    private final String dltTopic;

    public DeadLetterProducer(String bootstrapServers, String sourceTopic) {
        this.dltTopic = sourceTopic + ".DLT";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String key, String value, String failureReason) {
        ProducerRecord<String, String> record = new ProducerRecord<>(dltTopic, key, value);
        record.headers().add(new RecordHeader("dlt-failure-reason",
                failureReason.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("dlt-source-topic",
                dltTopic.replace(".DLT", "").getBytes(StandardCharsets.UTF_8)));

        producer.send(record, (meta, ex) -> {
            if (ex != null) {
                log.error("Failed to send to DLT topic={} key={}", dltTopic, key, ex);
            } else {
                log.warn("Moved to DLT topic={} partition={} offset={} key={} reason={}",
                        meta.topic(), meta.partition(), meta.offset(), key, failureReason);
            }
        });
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }
}
