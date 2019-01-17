package de.predic8.f_streams;

import de.predic8.ScheduleUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class VerkaeufeProducer {
    static final Map<Long, String> wareToWarengruppe = new HashMap<>();

    static String[] maerkte = { "Bonn", "Berlin", "Hamburg", "Köln", "Düsseldorf"};

    static String[] warengruppen = { "Food", "Zeitschriften", "Getränke"};

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        KafkaProducer<Long, Verkauf> producer = new KafkaProducer(props, new LongSerializer(), new JsonPOJOSerializer<Verkauf>() );
        ScheduleUtil.scheduleProduction(producer, VerkaeufeProducer::createRecord, 1000);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> { producer.close(); }));
    }

    private static ProducerRecord<Long, Verkauf> createRecord() {
        String markt = maerkte[random(4)];
        final long ware = random(10);
        String gruppe = wareToWarengruppe.computeIfAbsent(ware, l -> warengruppen[random(warengruppen.length-1)]);
        Verkauf verkauf = new Verkauf(ware,random(20), gruppe, markt);

        System.out.printf("created %s%n", verkauf);
        return new ProducerRecord<>("verkaeufe", verkauf.getWare(), verkauf);
    }

    public static int random(int max) {
        return ThreadLocalRandom.current().nextInt(1, max + 1);
    }
}
