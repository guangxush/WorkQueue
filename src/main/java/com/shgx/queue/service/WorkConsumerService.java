package com.shgx.queue.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: guangxush
 * @create: 2019/08/24
 */
@Service
@Slf4j
public class WorkConsumerService {

    /**
     * 线程池维护线程的最大数量
     */
    private static final int MAX_POOL_SIZE = 10;

    @Autowired
    private ThreadPoolManager threadPoolManager;

    @Autowired
    private WorkProducerService workProducerService;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    @Bean("ackContainerFactory")
    public ConcurrentKafkaListenerContainerFactory ackContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerProps()));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerProps()));
        return factory;
    }


    @KafkaListener(id = "test-consumer-group", topics = "business",containerFactory = "ackContainerFactory")
    public void ackListener(ConsumerRecord record, Acknowledgment ack) {
        log.info("Receive Business Number :------------"+record.value());
        if(threadPoolManager.getActiveCount()< MAX_POOL_SIZE){
            threadPoolManager.addOrders(record.value().toString());
            ack.acknowledge();
        }else{
            //未被消费的消息重新发到队列
            workProducerService.sendMessage(record.value().toString());
        }
    }

//    /**
//     * 20:53:00开始消费
//     */
//    @Scheduled(cron = "0 30 21 * * ?")
//    public void startListener() {
//        log.info("开启监听");
//        MessageListenerContainer container = registry.getListenerContainer("ack");
//        if (!container.isRunning()) {
//            container.start();
//        }
//        //恢复
//        container.resume();
//    }
//
//    /**
//     * 20:54:00结束消费
//     */
//    @Scheduled(cron = "0 50 21 * * ?")
//    public void shutdownListener() {
//        log.info("关闭监听");
//        //暂停
//        MessageListenerContainer container = registry.getListenerContainer("ack");
//        container.pause();
//    }
//
//
//    /**
//     * kafka监听工厂
//     *
//     * @param configurer
//     * @return
//     */
//    @Bean("batchFactory")
//    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
//            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
//            ConsumerFactory consumerFactory) {
//        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory);
//        //开启批量消费功能
//        factory.setBatchListener(true);
//        //不自动启动
//        factory.setAutoStartup(false);
//        configurer.configure(factory, consumerFactory);
//        return factory;
//    }
}
