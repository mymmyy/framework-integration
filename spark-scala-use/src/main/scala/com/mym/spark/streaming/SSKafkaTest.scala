package com.mym.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SSKafkaTest {

  def main(args: Array[String]): Unit = {


    //spark stream 从kafka读取数据，数据默认使用avro协议

    //定义zk
    val zookeeper = "192.168.31.201:2181";
    //定义kafka消费组id
    val groupid = "teststreaming";
    //定义topic
    val kafkaTopic:Map[String, Int]=Map[String, Int]("Streaming_topic"->1);

    val conf = new SparkConf();
    conf.setAppName("streaming-test");
    conf.setMaster("local[2]");

    val ssc:StreamingContext = new StreamingContext(conf, Seconds.apply(2));

    val stream = KafkaUtils.createStream(ssc, zookeeper, groupid, kafkaTopic, StorageLevel.MEMORY_ONLY)
      .map(recond => recond._2);

    stream.foreachRDD{
      x=>
        x.collect().foreach(println);
    }

    //开始运行
    ssc.start();
    //计算完毕退出
    ssc.awaitTermination();
  }

}
