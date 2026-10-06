package com.msr.consumer.controller;

import com.msr.consumer.model.Family;
import com.msr.consumer.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaService kafkaService;

    @GetMapping("/get-family")
    public ResponseEntity<String> getFamily(){
        String response=kafkaService.getMessage();
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
