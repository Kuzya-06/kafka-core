# kafka-core
Настройка кафки, продюсера, консюмера. Смотреть коммиты 
# Producer

# Consumer
## Параметры
### устанавливаются параметры конфигурации для Kafka Consumer
#### 1. GROUPIDCONFIG

```Java
properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
```

• GROUP_ID_CONFIG: Этот параметр определяет идентификатор группы потребителей. Все потребители, которые имеют одинаковый group.id, будут рассматриваться как часть одной группы и будут делить нагрузку по обработке сообщений из топиков Kafka. Это означает, что если несколько экземпляров вашего приложения (потребителей) запущены с одним и тем же group.id, Kafka будет распределять сообщения между ними, что позволяет добиться параллелизма и повышенной производительности.

#### 2. GROUPINSTANCEID_CONFIG

```Java
properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-instance-id");
```

• GROUP_INSTANCE_ID_CONFIG: Этот параметр используется в контексте управления состоянием и согласованности в группах потребителей. Он позволяет задать уникальный идентификатор для конкретного экземпляра потребителя в группе. Это особенно полезно в сценариях, когда вы хотите использовать функции, такие как статическая членственность (static membership), которые позволяют избежать повторной регистрации экземпляров при сбоях или перезапусках.

### Применение

• Группы потребителей: Использование GROUP_ID_CONFIG позволяет вам организовать ваши потребители в группы, что обеспечивает балансировку нагрузки и отказоустойчивость. Если один из потребителей в группе выходит из строя, другие могут продолжить обработку сообщений.

• Статическая членственность: GROUP_INSTANCE_ID_CONFIG позволяет вашему приложению поддерживать более устойчивую архитектуру, так как при сбоях и восстановлении экземпляры могут автоматически восстанавливаться без необходимости повторной регистрации, что снижает время простоя.
### Заключение

Эти настройки являются важными для управления поведением вашего Kafka Consumer и обеспечения его эффективной работы в распределённых системах. Выбор правильного group.id и использование group.instance.id может значительно повлиять на производительность и надёжность вашего приложения.

-------------
## Методы
### Параметры метода seek()
#### consumer.seek(new TopicPartition("test-topic", 1), new OffsetAndMetadata(5)); 
1. new TopicPartition("test-topic", 1):

   • TopicPartition: Это объект, который представляет собой комбинацию топика и его раздела (partition). В данном случае вы указываете, что хотите работать с разделом 1 топика "test-topic".

   • "test-topic": Имя топика, из которого вы хотите читать сообщения.

   • 1: Индекс раздела. В Kafka разделы нумеруются с нуля, так что это второй раздел (раздел 1).

2. new OffsetAndMetadata(5):

   • OffsetAndMetadata: Этот класс используется для представления смещения и связанных с ним метаданных. В данном случае вы указываете, что хотите установить смещение на 5.

   • 5: Смещение, на которое вы хотите переместить указатель чтения. Это означает, что следующий прочитанный элемент будет тем, который находится на пятом смещении в разделе 1.

▎Что делает этот код?

Когда вы вызываете consumer.seek(...), вы перемещаете указатель чтения для указанного раздела топика на заданное смещение. Это может быть полезно в различных сценариях:

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

