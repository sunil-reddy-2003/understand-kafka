package com.msr.producer.controller;

import com.msr.producer.model.Family;
import com.msr.producer.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaService kafkaService;

    @PostMapping("/add-family")
    public ResponseEntity<String> addFamily(@RequestBody Family family){
        String response=kafkaService.sendMessage(family);
        return new ResponseEntity<String>(response, HttpStatus.OK);
    }
}
