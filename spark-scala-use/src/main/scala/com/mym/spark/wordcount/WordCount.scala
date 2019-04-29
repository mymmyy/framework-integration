package com.mym.spark.wordcount
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    /**
      * 第一步：创建spark的创建对象sparkConf,设置spark运行时的配置信息
      */
    val conf = new SparkConf();
    conf.setAppName("wordcount");   //设置名称，在程序运行的监控界面可见
    conf.setMaster("local[2]");     //设置运行模式，在本地运行设置为local[k].如果是运行在集群中，以standalone模式运行，需要使用spark://Host:ost

    /**
      * 第二步：创建SparkContext对象，该对象是spark程序所有功能的唯一入口
      * sc的核心作用：初始化应用程序所需要的核心组件：DAGScheduler TaskScheduler *SchedulerBackend
      * 还会负责spark程序向master注册程序
      */
    val sc = new SparkContext(conf);

    /**
      * 第三步：根据具体的数据来源等通过SparkContext来创建rdd
      * rdd的创建方式，外部来源，通过scala的集合使用然后产生rdd，通过rdd产生rdd
      */
    val line = sc.textFile("spark-scala-use/test.log");

    /**
      * 用一些函数来计算
      */
    val words = line.flatMap(_.split(",")).flatMap(_.split(" ")).filter(word => word != " ");
    val pairs = words.map(word => (word,1));
    val wordCount = pairs.reduceByKey(_+_);
    val result = wordCount.collect();
    result.foreach(println);
    sc.stop();
  }

}
