package de.predic8;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;

public class SimpleConsumer {
    private SimpleConsumer() {}

    public static <K, V> KafkaConsumer<K, V> startConsumer(String groupId, String topic) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(SESSION_TIMEOUT_MS_CONFIG, "30000");
//        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topic));

        System.out.println("Consumer gestartet!");
        return consumer;
    }

    public static <K, V> void consumeForever(KafkaConsumer<K, V> kafkaConsumer, Consumer<ConsumerRecord<K,V>> consume) {
        consumerWhile(kafkaConsumer, () -> true, consume);
    }

    public static <K, V> void consumerWhile(KafkaConsumer<K,V> kafkaConsumer, Supplier<Boolean> goOn, Consumer<ConsumerRecord<K,V>> consume) {
        while (goOn.get()) {
            ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofSeconds(1));

            records.forEach(consume::accept);
        }
    }

    public static <K,V> Consumer<ConsumerRecord<K, V>> printRecordInfo() {
        return record -> System.out.printf("offset= %d, key= %s, value= %s%n", record.offset(), record.key(), record.value());
    }
}
