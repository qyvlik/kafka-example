package io.github.qyvlik.kafkaexample.modules.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumersService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @KafkaListener(topics = {"topic1-0", "topic2-0", "topic3-0"}, groupId = "#{topicGroupId}")
    public void processMessage(ConsumerRecord<Integer, String> record) {
        logger.debug("kafka processMessage start");
        logger.info("processMessage, topic:{}, offset:{}, msg:{}", record.topic(), record.offset(), record.value());
        logger.debug("kafka processMessage end");
    }

}
