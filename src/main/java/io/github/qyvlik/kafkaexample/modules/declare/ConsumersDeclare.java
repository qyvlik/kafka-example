package io.github.qyvlik.kafkaexample.modules.declare;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

public class ConsumersDeclare {

    public void declare(ConsumerFactory<Object, Object> consumerFactory,
                        String topic,
                        int partition,
                        Long initialOffset,
                        MessageListener<String, String> messageListener) {

        ContainerProperties containerProperties = new ContainerProperties(
                new TopicPartitionInitialOffset(topic, partition, initialOffset));
        containerProperties.setMessageListener(messageListener);

        ConcurrentMessageListenerContainer container =
                new ConcurrentMessageListenerContainer<>(
                        consumerFactory,
                        containerProperties);
        container.setConcurrency(1);
        container.start();
    }

}
