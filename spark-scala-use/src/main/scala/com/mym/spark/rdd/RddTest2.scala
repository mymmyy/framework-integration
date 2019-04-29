package com.mym.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RddTest2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setMaster("local[2]");
    conf.setAppName("rddTest2");
    val sc  =new SparkContext(conf);

    val rddIn = sc.makeRDD(Array(1,2,3,4));
    val rddInt2 = sc.makeRDD(Array(3,4,5,6,5));
    val rddStr = sc.makeRDD(List("abc","bcd","efg"));

    /**
      * 转换操作
      */
    //map操作
    val rddmap = rddIn.map{x => x + 2}.collect().mkString(",");
    println(rddmap);
    //filter操作
    println(rddIn.filter{ x => x > 3}.collect().mkString(","));
    //flatmap
    println(rddStr.flatMap{ x => x.split(",")}.collect().mkString("-"));
    //distinct 去重
    println(rddInt2.distinct().collect().mkString(","));
    //union
    println(rddIn.union(rddInt2).collect().mkString(","))
    //intersection
    println(rddIn.intersection(rddInt2).collect().mkString(","));
    //substract
    println(rddIn.subtract(rddInt2).collect().mkString(","));


    println("------------------------------------------------------------")

    /**
      * 行为
      */
    //count
    println(rddIn.count());
    //countByValue
    println(rddInt2.countByValue());
    //reduce 将rdd中元素前两个传给输入函数，产生一个新的return值，新产生的值与下一个rdd元素组成两个元素，再传入函数
    println(rddIn.reduce{ (x,y) => x+y});
    //reduceBykey 就是对元素为kv的rdd中key相同的元素的value进行函数操作
    val kvrdd = sc.parallelize(List((1,2),(1,4),(3,5)));
    println(kvrdd.reduceByKey((x,y) => x + y).collect().mkString("-"));
    // fold aggregate

    //foreach
    rddInt2.foreach{x => println(x)};

  }

}
