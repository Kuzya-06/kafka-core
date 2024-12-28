package ru.iteco.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.iteco.producer.KafkaProducerApp;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

public class KafkaConsumerAutoOffsetApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);

    public static void main(String[] args) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Создали новую группу "my-group-id-2"
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id-2");
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-instance-id");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.subscribe(Pattern.compile("test-topic"));


            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            StreamSupport.stream(records.spliterator(), false)
                    .forEach(record -> {
                        LOGGER.info("\nRecord: {}", record);
//                        consumer.commitSync(Map.of(
//                                new TopicPartition(record.topic(), record.partition()),
//                                new OffsetAndMetadata(record.offset()+1))); // синхронное фиксация смещения
//
//                        consumer.commitSync();

//                        consumer.commitAsync(); // из разных партиций считывает
/**
 * происходит асинхронная фиксация смещений (offsets) для Kafka Consumer.
 *
 */
                        consumer.commitAsync(Map.of(
                                        new TopicPartition(record.topic(), record.partition()),
                                        new OffsetAndMetadata(record.offset() + 1)), (offsets, exception) -> {
                                    LOGGER.info("Offsets: {}", offsets);
                                }
                        );

                    });
        }

    }
}

class MyConsumerRebalancedListener3 implements ConsumerRebalanceListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);
    private final KafkaConsumer<String, String> consumer;

    MyConsumerRebalancedListener3(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /**
     * вызывается, когда партиции, к которым был подписан потребитель, отзываются. В этом методе логируется
     * информация о том, какие партиции были отозваны.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now need to be revoked (may not
     *                   include all currently assigned partitions, i.e. there may still be some partitions left)
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.info("Partitions revoked - отозван: {}", partitions);
    }

    /**
     * вызывается, когда партиции назначаются потребителю. Здесь также логируется информация о назначенных партициях.
     *
     * @param partitions The list of partitions that are now assigned to the consumer (previously owned partitions will
     *                   NOT be included, i.e. this list will only include newly added partitions)
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOGGER.info("Partitions assigned - назначен: {}", partitions);
//        if (partitions.contains(new TopicPartition("test-topic", 1))) {
//            consumer.seek(new TopicPartition("test-topic", 1),
//                    new OffsetAndMetadata(10));
//        }
    }
}