package io.github.qyvlik.kafkaexample.modules.producers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class ProducersService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    public void setKafkaTemplate(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topic, String data) {
        logger.debug("kafka sendMessage start");

        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(topic, data);

        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                logger.error("kafka sendMessage error, ex = {}, topic = {}, data = {}", ex, topic, data);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                logger.debug("kafka sendMessage success topic:{}, data:{}",
                        result.getProducerRecord().topic(), result.getProducerRecord().value());
            }
        });
        logger.debug("kafka sendMessage end");
    }

}
