package ru.iteco.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.iteco.producer.KafkaProducerApp;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.StreamSupport;

public class KafkaConsumerApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);

    public static void main(String[] args) {
        System.out.println("Hello Consumer!");

        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.assign(List.of(
                    new TopicPartition("test-topic", 0),
                    new TopicPartition("test-topic", 1),
                    new TopicPartition("test-topic", 2)));

//            // Отмените подписку на темы, на которые в данный момент подписаны, с помощью подписки (Коллекция) или подписки
//            // (Шаблон). При этом также удаляются все разделы, назначенные напрямую с помощью функции Assign(Collection).
//            consumer.unsubscribe();

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            StreamSupport.stream(records.spliterator(), false)
                    .forEach(record -> LOGGER.info("\nRecord: {}", record));

        }

    }

}
