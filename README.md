# kafka-core

Настройка кафки, продюсера, консюмера. Смотреть коммиты.  
Запустить Docker
```bash
docker run -d --name broker apache/kafka:latest
```
```bash
docker compose up
```
```bash
cd c://Kafka/kafka_2.13-3.7.0/bin
```
Создать топик:
```bash
./kafka-topics.sh --bootstrap-server localhost:29092 --create --topic test-topic
```
Запустить продюсер с топиком test-topic
```bash
./kafka-console-producer.sh --bootstrap-server localhost:29092 --topic test-topic
```
Прочитайте события в теме теста с начала журнала:  
```bash
./kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic test-topic --from-beginning

```
# Producer

# Consumer

## Параметры

### устанавливаются параметры конфигурации для Kafka Consumer

#### 1. GROUPIDCONFIG

```Java
properties.put(ConsumerConfig.GROUP_ID_CONFIG,"my-group-id");
```

• GROUP_ID_CONFIG: Этот параметр определяет идентификатор группы потребителей. Все потребители, которые имеют одинаковый
group.id, будут рассматриваться как часть одной группы и будут делить нагрузку по обработке сообщений из топиков Kafka.
Это означает, что если несколько экземпляров вашего приложения (потребителей) запущены с одним и тем же group.id, Kafka
будет распределять сообщения между ними, что позволяет добиться параллелизма и повышенной производительности.

#### 2. GROUPINSTANCEID_CONFIG

```Java
properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"my-instance-id");
```

• GROUP_INSTANCE_ID_CONFIG: Этот параметр используется в контексте управления состоянием и согласованности в группах
потребителей. Он позволяет задать уникальный идентификатор для конкретного экземпляра потребителя в группе. Это особенно
полезно в сценариях, когда вы хотите использовать функции, такие как статическая членственность (static membership),
которые позволяют избежать повторной регистрации экземпляров при сбоях или перезапусках.

### Применение

• Группы потребителей: Использование GROUP_ID_CONFIG позволяет вам организовать ваши потребители в группы, что
обеспечивает балансировку нагрузки и отказоустойчивость. Если один из потребителей в группе выходит из строя, другие
могут продолжить обработку сообщений.

• Статическая членственность: GROUP_INSTANCE_ID_CONFIG позволяет вашему приложению поддерживать более устойчивую
архитектуру, так как при сбоях и восстановлении экземпляры могут автоматически восстанавливаться без необходимости
повторной регистрации, что снижает время простоя.

### Заключение

Эти настройки являются важными для управления поведением вашего Kafka Consumer и обеспечения его эффективной работы в
распределённых системах. Выбор правильного group.id и использование group.instance.id может значительно повлиять на
производительность и надёжность вашего приложения.

-------------

## Методы

### Параметры метода seek()

#### consumer.seek(new TopicPartition("test-topic", 1), new OffsetAndMetadata(5));

1. new TopicPartition("test-topic", 1):

   • TopicPartition: Это объект, который представляет собой комбинацию топика и его раздела (partition). В данном случае
   вы указываете, что хотите работать с разделом 1 топика "test-topic".

   • "test-topic": Имя топика, из которого вы хотите читать сообщения.

   • 1: Индекс раздела. В Kafka разделы нумеруются с нуля, так что это второй раздел (раздел 1).

2. new OffsetAndMetadata(5):

   • OffsetAndMetadata: Этот класс используется для представления смещения и связанных с ним метаданных. В данном случае
   вы указываете, что хотите установить смещение на 5.

   • 5: Смещение, на которое вы хотите переместить указатель чтения. Это означает, что следующий прочитанный элемент
   будет тем, который находится на пятом смещении в разделе 1.

▎Что делает этот код?

Когда вы вызываете consumer.seek(...), вы перемещаете указатель чтения для указанного раздела топика на заданное
смещение. Это может быть полезно в различных сценариях:

• Перезапуск обработки сообщений: Если вы хотите перезапустить обработку сообщений с определённого смещения.

• Пропуск сообщений: Если вам нужно пропустить определённые сообщения и начать чтение с другого места.

• Тестирование: При тестировании вы можете захотеть начать чтение с определённого места в топике.

▎Пример использования

Вот пример, как это может выглядеть в контексте полного кода:

```Java
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Properties;

public class KafkaSeekExample {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("test-topic"));

        // Пример использования seek
        TopicPartition partition = new TopicPartition("test-topic", 1);
        consumer.assign(Collections.singletonList(partition));

        // Установка смещения
        consumer.seek(partition, new OffsetAndMetadata(5));

        // Чтение сообщений начиная с установленного смещения
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed message with key %s and value %s from partition %d at offset %d%n",
                        record.key(), record.value(), record.partition(), record.offset());
            }
        }
    }
}
```

▎Заключение

Использование метода seek в Kafka Consumer позволяет вам точно контролировать, откуда начинать чтение сообщений. Это
может быть очень полезно в различных сценариях обработки данных.

#### consumer.seekToBeginning(List.of(new TopicPartition("test-topic", 2)));

#### consumer.seekToEnd(List.of(new TopicPartition("test-topic", 2)));

#### Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(Map.of(new TopicPartition("test-topic", 2), offsetForTimes));

#### consumer.commitAsync(Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)), (offsets, exception) -> { LOGGER.info("Offsets: {}", offsets); } );
Происходит асинхронная фиксация смещений (offsets) для Kafka Consumer.
##### ▎Разбор кода

##### 1. consumer.commitAsync(...):  
   • Этот метод используется для асинхронной фиксации смещений, что позволяет вам подтвердить, что сообщения были 
   обработаны и больше не должны быть прочитаны повторно.  
   • В отличие от commitSync(), который блокирует выполнение до завершения операции, commitAsync() выполняет 
   фиксацию в фоновом режиме и позволяет продолжать выполнение программы.  
##### 2. Map.of(...):  
   • Этот метод создает неизменяемую карту (Map) с одним элементом. В данном случае ключом является объект 
   TopicPartition, а значением — объект OffsetAndMetadata.  
   • new TopicPartition(record.topic(), record.partition()): Создаёт объект TopicPartition, указывающий на топик и 
   раздел, из которого был прочитан текущий record.  
   • new OffsetAndMetadata(record.offset() + 1): Создаёт объект OffsetAndMetadata, который указывает на смещение, на 
   одно большее, чем текущее (то есть вы фиксируете смещение следующего сообщения после текущего).  

##### 3. (offsets, exception) -> { LOGGER.info("Offsets: {}", offsets); }:

   • Это лямбда-выражение, которое является коллбеком, вызываемым после завершения операции фиксации смещений.

   • Параметр offsets содержит информацию о зафиксированных смещениях, а exception будет содержать информацию об ошибке, если она произошла.

   • Внутри коллбека происходит логирование зафиксированных смещений.

##### ▎Почему не отрабатывает LOGGER.info("Offsets: {}", offsets)?

Если LOGGER.info("Offsets: {}", offsets) не отрабатывает, это может происходить по нескольким причинам:

1. Исключение при фиксации: Если произошла ошибка во время фиксации смещений (например, если Consumer не подключен или произошла ошибка сети), то параметр exception будет ненулевым. В таком случае логирование может не срабатывать, если вы не проверяете наличие исключения.

```Java
if (exception != null) {
LOGGER.error("Error committing offsets", exception);
} else {
LOGGER.info("Offsets: {}", offsets);
}
```

2. Асинхронная природа: Поскольку метод commitAsync выполняется асинхронно, если ваша программа завершается до того, как коллбек будет вызван, вы не увидите вывод лога. Убедитесь, что ваше приложение продолжает работать достаточно долго для выполнения коллбека.

3. Конфигурация логирования: Проверьте настройки вашего логирования. Возможно, уровень логирования для INFO не включен или неправильно настроен.

4. Необработанные исключения: Если в процессе обработки сообщений возникают необработанные исключения, это может привести к завершению работы программы до вызова коллбека.

##### ▎Рекомендации

1. Проверка исключений: Добавьте проверку на наличие исключений в коллбек и логируйте их.

2. Убедитесь в работе приложения: Убедитесь, что ваше приложение продолжает работать после вызова commitAsync, чтобы дать возможность коллбеку выполниться.

3. Настройка логирования: Проверьте настройки вашего логирования, чтобы убедиться, что сообщения уровня INFO выводятся корректно.

Вот пример, как можно улучшить ваш код с обработкой исключений:

```Java
consumer.commitAsync(Map.of(
new TopicPartition(record.topic(), record.partition()),
new OffsetAndMetadata(record.offset() + 1)),
(offsets, exception) -> {
if (exception != null) {
LOGGER.error("Error committing offsets", exception);
} else {
LOGGER.info("Offsets: {}", offsets);
}
});
```

####


####