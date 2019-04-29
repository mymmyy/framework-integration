package com.mym.kafka.consumer;

import com.mym.kafka.producer.KafkaProducerTest;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.*;

public class KafkaConsumerTest {

    private final ConsumerConnector consumer;

    public KafkaConsumerTest(){
        Properties params = new Properties();
        params.put("zookeeper.connect","192.168.31.201:2181");
        params.put("group.id","testGroup");
        params.put("serializer.class","kafka.serializer.StringEncoder");

        ConsumerConfig consumerConfig = new ConsumerConfig(params);
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public void consumer(){
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KafkaProducerTest.TEST_TOPIC, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> messageStreams = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = messageStreams.get(KafkaProducerTest.TEST_TOPIC).get(0);

        ConsumerIterator<String, String> iterator = stream.iterator();
        while(iterator.hasNext()){
            MessageAndMetadata<String, String> next = iterator.next();
            System.out.println("接收到消息为："+next.message());
        }


    }

}
