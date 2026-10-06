package com.msr.consumer.service;

import com.msr.consumer.model.Family;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    private String message;

    @KafkaListener(topics = "msr",groupId = "fifth-child")
    public void consume(Family family){
        message= "got the family details :"+family;
        System.out.println(message);
    }

    public String getMessage() {
        return message;
    }
}
