package com.capitec.kafka.orderservice;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.*;

public class OrderServiceApp {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceApp.class);

    public static void main(String[] args) throws Exception {
        Properties env = loadConfig();
        String bootstrapServers    = env.getProperty("bootstrap.servers");
        String orderTopic      = env.getProperty("source.topic",       "order-created");
        String customerTopic   = env.getProperty("customer.topic",    "customer-created");
        String cancelledTopic  = env.getProperty("cancelled.topic",   "order-cancelled");
        String paymentTopic    = env.getProperty("payment.topic",     "payment-init");
        String groupId       = env.getProperty("group.id",        "order-service-group");
        String dbPath              = env.getProperty("db.path",                      "/data/orders.db");
        int    uiPort              = Integer.parseInt(env.getProperty("ui.port",     "8081"));

        Connection db = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        OrderRepository    orderRepo    = new OrderRepository(db);
        CustomerRepository customerRepo = new CustomerRepository(db);
        db.createStatement().execute("PRAGMA journal_mode=WAL");

        PaymentProducer     payment  = new PaymentProducer(bootstrapServers, paymentTopic);
        NotificationService notifier = new NotificationService();
        OrderUiServer       ui       = new OrderUiServer(uiPort, orderRepo, customerRepo);
        ui.start();

        List<String> topics = Arrays.asList(orderTopic, customerTopic, cancelledTopic);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildConsumerProps(bootstrapServers, groupId));
        consumer.subscribe(topics);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal — waking consumer...");
            consumer.wakeup();
        }));

        log.info("Order service started. topics={} group={}", topics, groupId);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        if      (customerTopic.equals(record.topic()))  processCustomer(record, customerRepo);
                        else if (cancelledTopic.equals(record.topic())) processCancellation(record, orderRepo);
                        else                                            processOrder(record, orderRepo, customerRepo, payment, notifier);
                    } catch (Exception e) {
                        log.error("Failed to process record topic={} key={}", record.topic(), record.key(), e);
                    }
                    offsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                    );
                }
                consumer.commitSync(offsets);
                payment.flush();
            }
        } catch (WakeupException e) {
            log.info("Consumer woken up for shutdown.");
        } finally {
            consumer.commitSync();
            consumer.close();
            payment.close();
            db.close();
            log.info("Order service shut down cleanly.");
        }
    }

    private static void processCustomer(ConsumerRecord<String, String> record,
                                        CustomerRepository repo) throws Exception {
        String json = record.value();
        if (json == null || json.isBlank()) return;
        Customer c = new Customer();
        c.customerID     = JsonParser.getString(json, "customerID");
        c.firstName      = JsonParser.getString(json, "firstName");
        c.lastName       = JsonParser.getString(json, "lastName");
        c.idNumber       = JsonParser.getString(json, "idNumber");
        c.email          = JsonParser.getString(json, "email");
        c.cell           = JsonParser.getString(json, "cell");
        c.passwordHash   = JsonParser.getString(json, "passwordHash");
        c.customerNumber = JsonParser.getLong(json, "customerNumber");
        if (c.customerID == null) return;
        repo.upsert(c);
        log.info("Customer upserted id={} number={}", c.customerID, c.customerNumber);
    }

    // Schema: {"orderID":"...","customerID":"...","reason":"Customer changed mind","cancelledAt":"..."}
    private static void processCancellation(ConsumerRecord<String, String> record,
                                            OrderRepository orderRepo) throws Exception {
        String json    = record.value();
        String orderID = JsonParser.getString(json, "orderID");
        String reason  = JsonParser.getString(json, "reason");
        if (orderID == null) { log.warn("Cancellation missing orderID key={}", record.key()); return; }
        if (reason  == null) reason = "No reason provided";
        orderRepo.updateCancelled(orderID, reason);
        log.info("Order cancelled orderID={} reason={}", orderID, reason);
    }

    private static void processOrder(ConsumerRecord<String, String> record,
                                     OrderRepository orderRepo, CustomerRepository customerRepo,
                                     PaymentProducer payment, NotificationService notifier) throws Exception {
        String value = record.value();
        if (value == null || value.isBlank()) return;
        Order order = JsonParser.parseOrder(value);
        if (order == null || order.orderID == null) {
            log.warn("Could not parse order key={}", record.key()); return;
        }

        // Upsert handles both new orders and status updates from payment-processor.
        // All order lifecycle events flow through order-created, so a single upsert
        // (which updates status on conflict) is the complete handling logic.
        orderRepo.upsert(order);
        log.info("Order upserted orderID={} status={}", order.orderID, order.status);

        // Only CONFIRMED orders trigger the payment flow
        if ("CONFIRMED".equalsIgnoreCase(order.status)) {
            payment.sendPaymentInstruction(order);
            orderRepo.updateStatus(order.orderID, "PAYMENT-INIT");
            log.info("Payment instruction sent orderID={} → PAYMENT-INIT", order.orderID);
            Customer customer = customerRepo.findById(order.customerID);
            notifier.sendPaymentInstruction(order, customer);
        }
    }

    private static Properties buildConsumerProps(String bootstrapServers, String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,          "read_committed");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       30_000);
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,     300_000);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         50);
        return p;
    }

    private static Properties loadConfig() throws IOException {
        try (InputStream in = OrderServiceApp.class.getResourceAsStream("/service.properties")) {
            if (in == null) throw new IOException("service.properties not found");
            Properties p = new Properties(); p.load(in); return p;
        }
    }
}
