package ru.iteco;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);

    public static void main(String[] args) {
        System.out.println("Hello Producer!");
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            RecordMetadata recordMetadata = producer.send(new ProducerRecord<>("test-topic", "Один топик")).get();
            LOGGER.info("==============================");
            LOGGER.info("Metadate: {}", recordMetadata.toString());
            LOGGER.info("==============================");

            RecordMetadata metadata =
                    producer.send(new ProducerRecord<>("test-topic", "my-key", "один топик и ключ")).get();
            LOGGER.info("==============================");
            LOGGER.info("Metadate: {}",metadata.toString());
            LOGGER.info("==============================");

            metadata =
                    producer.send(new ProducerRecord<>("test-topic", 0, "my-key", "топик, партиция 0, ключ")).get();
            LOGGER.info("==============================");
            LOGGER.info("Metadate: {}",metadata.toString());
            LOGGER.info("==============================");

            metadata =
                    producer.send(new ProducerRecord<>("test-topic", 0, "my-key", "топик, партиция 0, ключ, Заголовки",
                            List.of(new RecordHeader("Foo", "Bar".getBytes())))).get();
            LOGGER.info("==============================");
            LOGGER.info("Metadate: {}", metadata.toString());
            LOGGER.info("==============================");

            metadata =
                    producer.send(new ProducerRecord<>("test-topic", 0, System.currentTimeMillis(), "my-key-with" +
                            "-timestamp",
                            "топик, " +
                                    "партиция 0, метка " +
                                    "времени, " +
                                    "ключ, Заголовки",
                            List.of(new RecordHeader("Foo", "Bar".getBytes())))).get();
            LOGGER.info("==============================");
            LOGGER.info("Metadate метка времени: {}", metadata.toString());
            LOGGER.info("==============================");

            metadata =
                    producer.send(new ProducerRecord<>("test-topic", 0, System.currentTimeMillis(), "my-key-with" +
                            "-timestamp",
                            "топик, " +
                                    "партиция 0, метка " +
                                    "времени, " +
                                    "ключ, Заголовки",
                            List.of(new RecordHeader("Foo", "Bar".getBytes())))).get();
            LOGGER.info("==============================");
            LOGGER.info("Metadate метка времени: {}", metadata.toString());
            LOGGER.info("==============================");


        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
