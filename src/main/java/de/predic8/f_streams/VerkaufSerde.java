package de.predic8.f_streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public enum VerkaufSerde implements Serde<Verkauf> {
    INSTANCE;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Verkauf> serializer() {
        return new JsonPOJOSerializer<Verkauf>();
    }

    @Override
    public Deserializer<Verkauf> deserializer() {
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("JsonPOJOClass", Verkauf.class);
        JsonPOJODeserializer<Verkauf>  deserializer = new JsonPOJODeserializer<Verkauf>();
        deserializer.configure(serdeProps, false);

        return deserializer;
    }
}
