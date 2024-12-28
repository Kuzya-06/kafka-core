package ru.iteco.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

public class KafkaConsumerSubscribeApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);

    public static void main(String[] args) {
        System.out.println("Hello Consumer!");

        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /**
         * • GROUP_ID_CONFIG: Этот параметр определяет идентификатор группы потребителей. Все потребители, которые имеют
         * одинаковый group.id, будут рассматриваться как часть одной группы и будут делить нагрузку по обработке сообщений
         * из топиков Kafka. Это означает, что если несколько экземпляров вашего приложения (потребителей) запущены с одним и
         * тем же group.id, Kafka будет распределять сообщения между ними, что позволяет добиться параллелизма и повышенной
         * производительности.
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");

        /**
         * • GROUP_INSTANCE_ID_CONFIG: Этот параметр используется в контексте управления состоянием и согласованности
         * в группах потребителей. Он позволяет задать уникальный идентификатор для конкретного экземпляра
         * потребителя в группе. Это особенно полезно в сценариях, когда вы хотите использовать функции, такие как
         * статическая членственность (static membership), которые позволяют избежать повторной регистрации
         * экземпляров при сбоях или перезапусках.
         */
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-instance-id");


        /**
         * 1. Создание потребителя:
         * Здесь создаётся экземпляр KafkaConsumer, который будет использоваться для чтения сообщений из Kafka.
         * Параметр properties содержит настройки, такие как адреса брокеров, настройки безопасности и параметры десериализации ключей и значений.
         */
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            /**
             * 2. Подписка на тему:
             *  Потребитель подписывается на тему с именем "test-topic". В качестве второго параметра передаётся
             *  объект класса MyConsumerRebalancedListener, который реализует интерфейс ConsumerRebalanceListener.
             *  Этот интерфейс позволяет обрабатывать события, связанные с перераспределением партиций.
             */
            consumer.subscribe(Pattern.compile("test-topic"), new MyConsumerRebalancedListener());

            /**
             * Этот метод вызывает блокирующий опрос Kafka для получения сообщений. В данном случае он будет ждать до
             * 5 секунд, чтобы получить доступные сообщения из Kafka. Если в течение этого времени сообщений не
             * будет, метод вернёт пустой объект ConsumerRecords.
             * • ConsumerRecords<String, String>: Это коллекция записей, полученных от Kafka. Она содержит все
             * сообщения, которые были получены за время, указанное в методе poll.
             */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            /**
             * 2. Обработка полученных записей
             * • records.spliterator(): Этот метод возвращает Spliterator, который позволяет итерироваться по
             * элементам в ConsumerRecords. Это необходимо для использования стримов.
             *
             * • StreamSupport.stream(..., false): Создаёт последовательный стрим из Spliterator. Второй аргумент
             * false указывает, что стрим не будет параллельным.
             *
             * • forEach(record -> LOGGER.info("\nRecord: {}", record)): Здесь происходит итерация по каждому
             * сообщению (записи) в стриме. Для каждого сообщения вызывается метод LOGGER.info(), который выводит
             * информацию о записи в лог. В данном случае выводится сама запись.
             */
            StreamSupport.stream(records.spliterator(), false)
                    .forEach(record -> LOGGER.info("\nRecord: {}", record));

        }

    }

}

/**
 * Обработка перераспределения партиций важна для обеспечения надёжности и корректности обработки сообщений. Когда
 * потребитель присоединяется к группе или выходит из неё, партиции могут быть перераспределены между членами группы.
 * Логирование этих событий позволяет отслеживать изменения и управлять состоянием обработки сообщений более эффективно.
 */
class MyConsumerRebalancedListener implements ConsumerRebalanceListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);

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
    }
}