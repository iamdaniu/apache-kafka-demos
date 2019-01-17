package de.predic8;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

public class StreamsUtil {
    public static KafkaStreams startStreams(Properties props, StreamsBuilder builder) {
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));
        streams.start();
        return streams;
    }
}
