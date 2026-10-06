package com.msr.producer.service;

import com.msr.producer.model.Family;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    @Autowired
    private KafkaTemplate<String, Family> kafkaTemplate;

    public String sendMessage(Family family){
        kafkaTemplate.send("msr","parents",family);
        return "Family details sent to Kafka server";
    }
}
