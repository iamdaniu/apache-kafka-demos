package de.predic8.a_simple;

import de.predic8.SimpleConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer_C {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = SimpleConsumer.startConsumer("log", "produktion");

        SimpleConsumer.consumeForever(consumer, SimpleConsumer.printRecordInfo());
    }
}
