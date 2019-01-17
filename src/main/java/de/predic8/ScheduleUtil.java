package de.predic8;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ScheduleUtil {
    static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

    public static <K,V> ScheduledFuture<?> scheduleProduction(KafkaProducer<K, V> producer, Supplier<ProducerRecord<K ,V>> recordSupplier, int rate) {
        return SCHEDULER.scheduleAtFixedRate(() -> producer.send(recordSupplier.get()), 0, rate, TimeUnit.MILLISECONDS);
    }
}
