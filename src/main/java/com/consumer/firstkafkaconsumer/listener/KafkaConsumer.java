package com.consumer.firstkafkaconsumer.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Service
public class KafkaConsumer {

    @KafkaListener(topics = "kafkaexample", containerFactory = "containerFactory")
    public void listenPEN_RE(@Payload String message,
                             @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partition,
                             @Header(KafkaHeaders.OFFSET) final int offset,
                             final Acknowledgment acknowledgment) throws InterruptedException {

        System.out.println(message);
        System.out.println(partition);
        System.out.println(offset);

        Thread.sleep(2000);
        acknowledgment.acknowledge();

    }

}
