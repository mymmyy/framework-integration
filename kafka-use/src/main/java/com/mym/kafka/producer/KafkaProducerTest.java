package com.mym.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducerTest {

    private final Producer<String, Object> producer;

//    public final static String TEST_TOPIC = "test_topic";
    public final static String TEST_TOPIC = "Streaming_topic";

    public KafkaProducerTest(){
        Properties params = new Properties();
        //kafka地址及端口
        params.put("metadata.broker.list","192.168.31.201:9092");
        //配置value的序列化类
        params.put("serializer.class","kafka.serializer.StringEncoder");
        //配置key的序列化类
        params.put("key.serializer.class","kafka.serializer.StringEncoder");
        //ack -1标识副本确认接手数据后就算完成。1标识leader成功收到数据且确认后。0标识不等确认
        params.put("request.required.acks","-1");

        producer = new Producer<String, Object>(new ProducerConfig(params));
    }

    public void produce(){
        int messageNo = 0;
        final int COUNT = 100000;
        while(messageNo++ < COUNT){
            String key = String.valueOf(messageNo);
            String value = String.valueOf("hello message is no:"+messageNo);
            producer.send(new KeyedMessage<String, Object>(TEST_TOPIC, key, value));
            System.out.println(key+" -> "+value);
        }
    }





}
