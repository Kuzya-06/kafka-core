package ru.iteco.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerIdempotentApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerIdempotentApp.class);

    public static void main(String[] args) {
        System.out.println("Hello Producer!");
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Отключение идемпотентности. Нужно отключить принудительно
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.FALSE.toString());


        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            RecordMetadata recordMetadata1 = producer.send(new ProducerRecord<>("test-topic", "Text Idempotent")).get();
            RecordMetadata recordMetadata2 =
                    producer.send(new ProducerRecord<>("test-topic", "Text Idempotent 2")).get();

            LOGGER.info("==============================");
            LOGGER.info("Metadate: {}", recordMetadata1.toString());
            LOGGER.info("==============================");

            LOGGER.info("==============================");
            LOGGER.info("Metadate: {}", recordMetadata2.toString());
            LOGGER.info("==============================");
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
// -------------------------------------------------------------------------
        // Альтернативно. Установить ACKS = 1 подтверждение от Лидера
        // Альтернативно. Установить ACKS = 0 не ждем подтверждение от Лидера. Очень интересен ответ в метаданных @-1
        // Альтернативно. Установить ACKS = -1 подтверждение от Лидера и реплик

////        properties.put(ProducerConfig.ACKS_CONFIG,"0");
////        properties.put(ProducerConfig.ACKS_CONFIG,"1");
//        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
//        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
//
//            RecordMetadata recordMetadata1 =
//                    producer.send(new ProducerRecord<>("test-topic", "Text Idempotent"),
//                            ((metadata, exception)->LOGGER.info("Metadata: {}", metadata))).get();
//
//            LOGGER.info("==============================");
//            LOGGER.info("Metadate: {}", recordMetadata1.toString());
//            LOGGER.info("==============================");


//        } catch (ExecutionException | InterruptedException e) {
//            throw new RuntimeException(e);
//        }
    }
}
