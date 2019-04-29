package com.mym.kafka;

import com.mym.kafka.consumer.KafkaConsumerTest;
import com.mym.kafka.producer.KafkaProducerTest;

public class MainClass {

    public static void main(String[] args) {
        KafkaProducerTest kafkaProducerTest = new KafkaProducerTest();
        kafkaProducerTest.produce();


//        KafkaConsumerTest kafkaConsumerTest = new KafkaConsumerTest();
//        kafkaConsumerTest.consumer();
    }

}
