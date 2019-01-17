package de.predic8;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

import static java.util.stream.Collectors.joining;

/**
 * Created by thomas on 31.10.16.
 */
public class LogRebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition>partitions) {
        System.out.printf("Revoked from: %n%s%n", partitions.stream().map(Object::toString).collect(joining(",")));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.printf("Assigned to: %n%s%n", partitions.stream().map(Object::toString).collect(joining(",")));
    }
}
