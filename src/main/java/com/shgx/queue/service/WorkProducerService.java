package com.shgx.queue.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


/**
 * @author: guangxush
 * @create: 2019/08/24
 */
@Service
public class WorkProducerService {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    public boolean sendMessage(String msg) {
        try {
            String result = kafkaTemplate.send("business", msg).get().toString();
            if (result != null) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }
}
