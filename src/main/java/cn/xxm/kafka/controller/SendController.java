package cn.xxm.kafka.controller;

import cn.xxm.kafka.config.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class SendController {

    @Autowired
    private KafkaProducer producer;

    @RequestMapping(value = "/send")
    public String send() {
        producer.send();
        return "{\"code\":0}";
    }
}