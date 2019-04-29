package com.mym.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RddTest {

  def main(args:Array[String]): Unit ={
    val conf = new SparkConf();
    conf.setAppName("rddTest");
    conf.setMaster("local[2]");
    val sc = new SparkContext(conf);

    /**
      * 使用makerRDD创建RDD 可以通过List或者Array来创建
      */
    val rdd0 = sc.makeRDD(List(1,2,3,4,5,6,7));
    val rdd1 = sc.makeRDD(Array(7,6,5,4,3,2,1));
    val rdd2 = rdd0.map(x => x*x);
    print(rdd2.collect().mkString(","));

    /**
      * 使用parallize创建rdd
      */
    val rdd3 = sc.parallelize(List(1,2,3),4);
    val rdd4 = rdd3.map(x => x + x);
    println(rdd4.collect().mkString("-"));

    /**
      * rdd本质是一个数组，因此可以用List和Array来创建
      */
    val lines = sc.textFile("spark-scala-use/test.log");
    val read = lines.flatMap(x => x.split(","));
    println(read.collect().mkString("--"));




  }

}
