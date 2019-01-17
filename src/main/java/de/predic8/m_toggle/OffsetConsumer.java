package de.predic8.m_toggle;

import de.predic8.OffsetBeginningRebalanceListener;
import de.predic8.SimpleConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static java.util.Collections.singletonList;

public class OffsetConsumer {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = SimpleConsumer.startConsumer("a", "toggle");

        consumer.subscribe( singletonList("toggle"), new OffsetBeginningRebalanceListener(consumer));

        SimpleConsumer.consumeForever(consumer, SimpleConsumer.printRecordInfo());
    }
}
