package de.predic8.f_streams;

import de.predic8.ScheduleUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.random;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class ContinuousNumberProducer {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaProducer<Integer, Double> producer = new KafkaProducer<>(props, new IntegerSerializer(), new DoubleSerializer());
        AtomicInteger id = new AtomicInteger();
        final ScheduledFuture<?> schedule = ScheduleUtil.scheduleProduction(producer,
                () -> new ProducerRecord<>("numbers-cont", id.getAndIncrement(), random() * 1000),
                1000);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            schedule.cancel(true);
            producer.close();
        }
        ));
    }
}
