package ru.iteco;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerTransactionalApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerTransactionalApp.class);

    public static void main(String[] args) {
        System.out.println("Hello Producer!");
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // в консоли выполнить команду
        // ./kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic test-topic --from-beginning
        // --isolation-level read_committed

        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-trasactional-id");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.initTransactions();
            producer.beginTransaction();

            RecordMetadata recordMetadata1 = producer.send(new ProducerRecord<>("test-topic", "Text 1")).get();
            RecordMetadata recordMetadata2 = producer.send(new ProducerRecord<>("test-topic", "Text 2")).get();


            LOGGER.info("==============================");
            LOGGER.info("Metadate: {}", recordMetadata1.toString());
            LOGGER.info("==============================");

            LOGGER.info("==============================");
            LOGGER.info("Metadate: {}", recordMetadata2.toString());
            LOGGER.info("==============================");

//            throw new IllegalArgumentException();

            producer.commitTransaction();
//            producer.abortTransaction();

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
