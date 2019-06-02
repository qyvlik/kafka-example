package io.github.qyvlik.kafkaexample.modules.declare;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

import java.util.HashMap;
import java.util.Map;

public class ConsumersDeclare {

    public void declare(MessageListener<String, String> messageListener,
                        String brokerAddress,
                        String groupId,
                        String topic) {
        ConcurrentMessageListenerContainer container =
                new ConcurrentMessageListenerContainer<>(
                        consumerFactory(brokerAddress, groupId),
                        containerProperties(topic, messageListener));
        container.setConcurrency(1);
        container.start();
    }

    public void declare(Map<String, Object> consumerConfigMap, MessageListener<String, String> messageListener) {

    }

    private ContainerProperties containerProperties(String topic, MessageListener<String, String> messageListener) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener(messageListener);
        return containerProperties;
    }

    private DefaultKafkaConsumerFactory<String, String> consumerFactory(String brokerAddress, String groupId) {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfig(brokerAddress, groupId),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private Map<String, Object> consumerConfig(String brokerAddress, String groupId) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        map.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return map;
    }

}
