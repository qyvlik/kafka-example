package io.github.qyvlik.kafkaexample.modules.rest;

import io.github.qyvlik.kafkaexample.modules.producers.ProducersService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducersController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ProducersService producersService;

    @RequestMapping(value = "api/v1/send", method = RequestMethod.GET)
    public String send() {
        logger.info("kafka sendMessage start");

        int counter = 1000;
        while (counter-- > 0) {
            producersService.sendMessage("topic1-0", "good-1:" + counter);
            producersService.sendMessage("topic2-0", "good-2:" + counter);
            producersService.sendMessage("topic3-0", "good-3:" + counter);
        }
        logger.info("kafka sendMessage end");

        return "{\"result\":\"success\"}";
    }
}
