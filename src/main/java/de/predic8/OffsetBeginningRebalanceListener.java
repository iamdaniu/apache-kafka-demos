package de.predic8;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class OffsetBeginningRebalanceListener implements ConsumerRebalanceListener {
    LogRebalanceListener logRebalanceListener = new LogRebalanceListener();

    private final KafkaConsumer<String, String> consumer;
    private boolean resetted;

    public OffsetBeginningRebalanceListener(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition>partitions) {
        logRebalanceListener.onPartitionsRevoked(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logRebalanceListener.onPartitionsAssigned(partitions);

        if (!resetted) {
            consumer.seekToBeginning(partitions);
            resetted = true;
        }
    }

}
