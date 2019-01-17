package de.predic8.m_toggle;

import de.predic8.f_streams.JsonPOJODeserializer;
import de.predic8.f_streams.JsonPOJOSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public enum ToggleSerde implements Serde<Toggle> {
    INSTANCE;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Toggle> serializer() {
        return new JsonPOJOSerializer<>();
    }

    @Override
    public Deserializer<Toggle> deserializer() {
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("JsonPOJOClass", Toggle.class);
        JsonPOJODeserializer<Toggle>  deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(serdeProps, false);

        return deserializer;
    }
}
