package ru.iteco.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.iteco.producer.KafkaProducerApp;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.StreamSupport;

public class KafkaConsumerSeekApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);

    public static void main(String[] args) {
        System.out.println("Hello Consumer!");

        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
//        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-instance-id");


        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.assign(List.of(
                    new TopicPartition("test-topic", 0),
                    new TopicPartition("test-topic", 1)
                    ,
                    new TopicPartition("test-topic", 2)
            ));

            /**
             * consumer.seek - позволяет установить смещение (offset) для конкретного раздела (partition) топика
             */
//            consumer.seek(new TopicPartition("test-topic", 1),
//                    new OffsetAndMetadata(5));
// Альтернатива seek()
//            consumer.seekToBeginning(List.of(new TopicPartition("test-topic", 2)));
//            consumer.seekToEnd(List.of(new TopicPartition("test-topic", 2)));

            //-------------------------------------
//            Long offsetForTimes = 1735391180402L; // 16 offset
            Long offsetForTimes = 1735391180405L; // 17 offset
            // очень затратное по времени.
            //TODO проверить время выполнения
            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                    consumer.offsetsForTimes(Map.of(new TopicPartition("test-topic", 0), offsetForTimes
                    ,new TopicPartition("test-topic", 1), offsetForTimes,
                            new TopicPartition("test-topic", 2), offsetForTimes));

            //------------------------------------------
            consumer.seek(new TopicPartition("test-topic", 2),
                    offsetsForTimes.get(new TopicPartition("test-topic", 2)).offset());

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            StreamSupport.stream(records.spliterator(), false)
                    .forEach(record -> LOGGER.info("\nRecord: {}", record));

            //------------------------------------------
            consumer.seek(new TopicPartition("test-topic", 0),
                    offsetsForTimes.get(new TopicPartition("test-topic", 0)).offset());

            ConsumerRecords<String, String> records1 = consumer.poll(Duration.ofSeconds(5));
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            StreamSupport.stream(records1.spliterator(), false)
                    .forEach(record -> LOGGER.info("\nRecord: {}", record));

            //------------------------------------------
            consumer.seek(new TopicPartition("test-topic", 1),
                    offsetsForTimes.get(new TopicPartition("test-topic", 1)).offset());

            ConsumerRecords<String, String> records2 = consumer.poll(Duration.ofSeconds(5));
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            StreamSupport.stream(records2.spliterator(), false)
                    .forEach(record -> LOGGER.info("\nRecord: {}", record));

        }

    }

}
