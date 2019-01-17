package de.predic8.m_toggle;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.predic8.f_streams.JsonPOJODeserializer;
import de.predic8.f_streams.JsonPOJOSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class ToggleProducer {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws InterruptedException {
        String[] values = new String[] { "feature1", "{\"name\":\"feature1\",\"description\":\"mein erstes feature\",\"enabled\":true}" };

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaProducer<String, Toggle> producer = new KafkaProducer(props, new StringSerializer(), new JsonPOJOSerializer<Toggle>());
        //        ScheduleUtil.scheduleProduction(producer, ToggleProducer::createRecord, 1000);
        final JsonPOJODeserializer jsonPOJODeserializer = new JsonPOJODeserializer();
        for (int i = 1; i < values.length; i += 2) {
            try {
                ProducerRecord<String, Toggle> data = new ProducerRecord<>("toggle", values[i - 1], mapper.readValue(values[i], Toggle.class));
                System.out.printf("Sending %s - %s%n", data.key(), data.value());
                producer.send(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
        }));
    }

    //    private static ProducerRecord<Long, Toggle> createRecord() {
    //        String gruppe = wareToWarengruppe.computeIfAbsent(ware, l -> warengruppen[random(warengruppen.length-1)]);
    //        Toggle toggle = new Toggle(ware,random(20), gruppe, markt);
    //
    //        System.out.printf("created %s%n", Toggle);
    //        return new ProducerRecord<>("toggle", Toggle.getName(), toggle);
    //    }

    public static int random(int max) {
        return ThreadLocalRandom.current().nextInt(1, max + 1);
    }
}
