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
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

/**
 * Если нам требуется найти какую-то позицию (какое-то смещение) в партиции при подписке, нам надо сначала дождаться
 * подписки на указанную партицию, и только после этого выставлять нужное смещение.
 */

public class KafkaConsumerRebalancerApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);

    public static void main(String[] args) {
        System.out.println("Hello Consumer!");

        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-instance-id");

        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.subscribe(Pattern.compile("test-topic"), new MyConsumerRebalancedListener2(consumer));


//            Long offsetForTimes = 1735391180405L; // 17 offset
//            var offsetsForTimes = consumer.offsetsForTimes(Map.of(new TopicPartition("test-topic", 1),
//                    offsetForTimes));
//
//            consumer.seek(new TopicPartition("test-topic", 1),
//                    offsetsForTimes.get(new TopicPartition("test-topic", 1)).offset());

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1)); // достаточно время
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); // не достаточное время
            // для перебалансировки и не получим данные
            StreamSupport.stream(records.spliterator(), false)
                    .forEach(record -> LOGGER.info("\nRecord: {}", record));

        }

    }

}

class MyConsumerRebalancedListener2 implements ConsumerRebalanceListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);
    private final KafkaConsumer<String, String> consumer;

    MyConsumerRebalancedListener2(KafkaConsumer<String, String> consumer) {
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
        if (partitions.contains(new TopicPartition("test-topic", 1))) {
            consumer.seek(new TopicPartition("test-topic", 1),
                    new OffsetAndMetadata(10));
        }
    }
}